"""Helpers for merging byte ranges and computing missing intervals.

All ranges use half-open offsets ``[start, end)`` so that the byte length
is simply ``end - start`` and two ranges touch when ``a.end == b.start``.
"""

from __future__ import annotations

from typing import Iterable, Sequence

Interval = tuple[int, int]


def merge_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    ordered = sorted((s, e) for s, e in intervals if e > s)
    merged: list[Interval] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def missing_intervals(
    received: Sequence[Interval], total_size: int
) -> list[Interval]:
    """Return the uncovered portions of ``[0, total_size)``."""
    gaps: list[Interval] = []
    cursor = 0
    for start, end in merge_intervals(received):
        if start > cursor:
            gaps.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < total_size:
        gaps.append((cursor, total_size))
    return gaps


def is_complete(received: Sequence[Interval], total_size: int) -> bool:
    return not missing_intervals(received, total_size)


def overlaps(received: Sequence[Interval], start: int, end: int) -> bool:
    for r_start, r_end in merge_intervals(received):
        if start < r_end and r_start < end:
            return True
    return False
