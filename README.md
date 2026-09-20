# Resumable File Upload API

面向弱网客户端的断点续传文件上传服务：Python + FastAPI + SQLite。

- 客户端先声明文件大小与整文件 SHA-256 创建上传会话；
- 分片可**按任意顺序、任意时间**提交，偏移使用半开区间 `[start, end)`；
- 服务端拒绝**越界**（`416`）、**与已收区间重叠**（`409`）以及长度不符（`400`）的分片；
- 状态接口实时返回已收合并区间与**缺失区间**，方便客户端断点续传；
- 仅当分片完整覆盖 `[0, total_size)` **且整文件 SHA-256 校验通过**时，才通过 `os.replace`
  原子发布成品；校验失败不会产生任何可下载文件（`422`，会话回到可上传状态）；
- 只有状态为 `completed` 的文件可下载；
- 上传会话按 TTL 过期，后台线程周期性清理数据库记录与暂存分片；
- 分片支持可选的 `X-Chunk-SHA256` 头做单分片完整性校验；
- 完全相同的分片重复提交是幂等的（返回 `200`，`duplicate: true`）。

## 一键启动（需要 Docker + Docker Compose）

```bash
docker compose up --build -d
```

服务监听宿主机 **8000 端口**：

- API 根地址：`http://localhost:8000`
- 健康检查：`http://localhost:8000/health`
- 交互式文档（Swagger UI）：`http://localhost:8000/docs`

停止并保留数据：`docker compose stop`；连同数据卷一起删除：`docker compose down -v`。

## 配置项

全部通过环境变量配置（`docker-compose.yml` 的 `environment:` 段），均有默认值：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `STORAGE_DIR` | `/data` | SQLite 与分片/成品文件的存储根目录（容器卷挂载点） |
| `DATABASE_PATH` | `$STORAGE_DIR/app.db` | SQLite 数据库文件路径 |
| `SESSION_TTL_SECONDS` | `86400`（1 天） | 上传会话有效期，超时的未完成会话会被清理 |
| `CLEANUP_INTERVAL_SECONDS` | `300`（5 分钟） | 后台过期清理轮询间隔 |
| `MAX_FILE_SIZE` | `0`（不限） | 允许的整文件最大字节数，超出在创建会话时返回 `413` |
| `MAX_CHUNK_SIZE` | `0`（不限） | 单个分片最大字节数，超出返回 `413` |

数据保存在 Docker 卷 `upload-data` 中（挂载到容器 `/data`）。

## API 概览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/sessions` | 创建上传会话 |
| `PUT` | `/sessions/{id}/chunks?start=&end=` | 上传一个二进制分片（body 即原始字节） |
| `GET` | `/sessions/{id}` | 查询状态（含 `received_ranges` / `missing_ranges`） |
| `PUT` | `/sessions/{id}/complete` | 覆盖完整且 SHA-256 匹配时原子完成 |
| `GET` | `/sessions/{id}/download` | 下载成品（仅 `completed` 可用） |
| `DELETE` | `/sessions/{id}` | 删除会话及其全部暂存/成品数据 |
| `GET` | `/health` | 健康检查 |

### 1. 创建会话

```bash
curl -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "total_size": 2048,
    "sha256": "9f86d081...（64 位十六进制）",
    "filename": "video.mp4",
    "content_type": "video/mp4"
  }'
```

响应（节选）：

```json
{
  "id": "f0f4910f...",
  "status": "uploading",
  "total_size": 2048,
  "received_bytes": 0,
  "received_ranges": [],
  "missing_ranges": [[0, 2048]],
  "complete": false,
  "expires_at": "2026-09-21T11:00:00+00:00"
}
```

### 2. 按任意顺序上传分片

偏移为半开区间：`start` 为起始字节（含），`end` 为结束字节（不含），
请求 body 必须恰好是 `end - start` 个字节。

```bash
# 先传中间，再传两头——顺序任意
curl -X PUT "http://localhost:8000/sessions/$SID/chunks?start=1024&end=2048" \
  --data-binary @part-tail.bin
curl -X PUT "http://localhost:8000/sessions/$SID/chunks?start=0&end=512" \
  --data-binary @part-head.bin
```

可选单分片校验：加请求头 `X-Chunk-SHA256: <该分片的 sha256>`。

每次成功上传后响应都会返回最新的 `received_ranges` 与 `missing_ranges`，
客户端断线重连后只需 `GET /sessions/{id}` 即可知道还要补传哪些区间。

拒绝规则：

- 区间越界（`end > total_size`、`start >= total_size` 等）→ `416 Range Not Satisfiable`
- 与已收分片重叠（含相同区间但内容不同）→ `409 Conflict`
- `end <= start`、body 长度与区间长度不符、超限 → `400 / 413`
- 会话已完成或已过期 → `409 / 410 Gone`

### 3. 查询缺失区间

```bash
curl http://localhost:8000/sessions/$SID
```

```json
{
  "status": "uploading",
  "received_bytes": 1536,
  "received_ranges": [[0, 512], [1024, 2048]],
  "missing_ranges": [[512, 1024]],
  "complete": false
}
```

### 4. 原子完成

```bash
curl -X PUT http://localhost:8000/sessions/$SID/complete
```

- 区间未覆盖完整 → `409`，响应体中带 `missing` 列表；
- 覆盖完整但 SHA-256 不匹配 → `422`，**不会发布任何成品文件**，会话恢复为 `uploading`；
- 成功 → `200`，状态变为 `completed`，响应含 `download_url`。

实现上先写入隐藏临时文件、边拼接边计算 SHA-256，校验通过后用
`os.replace()` 原子改名发布，并与数据库状态更新放在同一最终事务步骤中。

### 5. 下载

```bash
curl -OJ http://localhost:8000/sessions/$SID/download
```

仅 `completed` 会话可下载（`Content-Disposition` 带回原文件名，
`X-Content-SHA256` 头带整文件摘要）；未完成或校验失败者返回 `409`。

## 会话过期清理

- 创建会话时写入 `expires_at = now + SESSION_TTL_SECONDS`；
- 后台守护线程每 `CLEANUP_INTERVAL_SECONDS` 秒扫描一次，删除所有
  `status = uploading` 且已过期的会话：数据库记录（外键级联分片记录）与
  `staging/` 下的暂存分片文件一并清除；
- 已完成（`completed`）的会话不会被过期清理，可长期下载；
- 服务重启时会把崩溃残留的 `completing` 中间态回滚为 `uploading`，并清理遗留临时文件。

## 本地开发运行（不使用 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

端到端冒烟测试（覆盖越界/重叠拒绝、缺失区间、幂等重传、SHA-256 失败不留成品、
过期清理、空文件等）：

```bash
pip install httpx          # 仅测试需要
python tests/smoke_test.py
```

## 存储布局

```text
$STORAGE_DIR/
├── app.db                  # SQLite（WAL 模式）
├── staging/<session-id>/  # 已收分片（每片一个 .part 文件）
└── files/<session-id>     # 校验通过后原子发布的成品文件
```
