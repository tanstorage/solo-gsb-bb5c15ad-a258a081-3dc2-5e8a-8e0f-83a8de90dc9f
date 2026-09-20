FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    STORAGE_DIR=/data

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data && \
    useradd --create-home --uid 10001 appuser && \
    chown -R appuser:appuser /srv /data
USER appuser

EXPOSE 8000
VOLUME ["/data"]

# Single worker: the overlap/accounting lock is in-process. Horizontal
# scaling would need the database to hold the write lock (a single
# SQLite file still serialises, but a shared lock is simpler and safe).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
