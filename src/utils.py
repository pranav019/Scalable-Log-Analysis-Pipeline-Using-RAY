"""
Utilities : types, parsers, sliding-window aggregator helpers, config constants.
This module defines a LogLine dataclass, parsing helpers, and compact sliding window aggregation helper used by the AggregatorActor.
"""

from dataclasses import (
    dataclass,
    asdict,
)  # asdict : A helper function that converts a dataclass object into a dictionary.
from typing import Dict, Any, Optional, Tuple, List
import re
from datetime import datetime, timedelta
from collections import deque, defaultdict
import logging

logger = logging.getLogger("utils")

DEFAULT_CONFIG = {
    "detector": {
        "error_rate_threshold": 0.10,  # 10% errors across a window => anomaly
        "min_requests_threshold": 100,  # Only trigger if requets => this
        "window_seconds": 60,  # Sliding window size in seconds
        "bucket_seconds": 5,  # Bucket granularity
    }
}


@dataclass
class LogLine:
    """
    Structured representation of a parsed log line.

    Attributes:
        timestamp: Unix timestamp in seconds (float).
        service: Logical service name e.g., 'webapp'.
        endpoint: API endpoint or resource path.
        status: HTTP status code as int (e.g., 200, 404, 500).
        latency_ms: Response latency in milliseconds.
        user_id: Optional user identifier string.
        raw: Original raw log line (optional).
    """

    timestamp: float
    service: str
    endpoint: str
    status: int
    latency_ms: float
    user_id: Optional[str] = None
    raw: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return as JSON-serializable dict."""
        return asdict(self)


# Simple regex for combined log-like lines used in generator
_LOG_RE = re.compile(
    r"(?P<ts>[\d\-T:\.Z]+)\s+\|\s+(?P<service>\w+)\s+\|\s+(?P<endpoint>\S+)\s+\|\s+status=(?P<status>\d+)\s+\|\s+lat=(?P<lat>[\d\.]+)ms\s+\|\s+user=(?P<user>[\w\-]+)"
)


def parse_log_line(line: str) -> Optional[LogLine]:
    """
    Parse a log line produced by LogGenerator or similar.

    Args:
        line: Raw log line string.

    Returns:
        LogLine if parsing succeeded, otherwise None.
    """
    match = _LOG_RE.search(line)
    if not match:
        logger.debug("Failed to parse line: %s", line)
        return None

    try:
        ts_str = match.group("ts")
        # parse timestamp tolerant to ISO format
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        service = match.group("service")
        endpoint = match.group("endpoint")
        status = int(match.group("status"))
        latency = float(match.group("lat"))
        user = match.group("user")
        return LogLine(
            timestamp=ts,
            service=service,
            endpoint=endpoint,
            status=status,
            latency_ms=latency,
            user_id=user,
            raw=line,
        )

    except Exception as e:
        logger.exception("Exception parsing line: %s", e)
        return None


class SlidingWindowAggregator:
    """
    Sliding-window aggregator that buckets metrics by time windows.

    inside an Actor (single-threaded) to maintain sliding aggregates.

    Usage:
        swa = SlidingWindowAggregator(window_seconds=60, bucket_seconds=5)
        swa.add(timestamp, tags={...}, is_error=True)
        snapshot = swa.snapshot()
    """

    def __init__(self, window_seconds: int = 60, bucket_Seconds: int = 5) -> None:
        """_summary_
        Args:
            window_seconds: Width of sliding window in seconds.
            bucket_seconds: Bucket granularity in seconds.

        """

        if bucket_Seconds <= 0:
            raise ValueError("bucket sceonds must be positive")
        if window_seconds <= 0:
            raise ValueError("window_Seconds must be positive")

        self.window_Seconds = window_seconds
        self.bucket_Seconds = bucket_Seconds

        self.num_buckets = int((window_seconds + bucket_Seconds - 1) // bucket_Seconds)
        # buckets: deque of dicts where each dict is counters per (service, endpoint)
        self.buckets: deque = deque(maxlen=self.num_buckets)
        for _ in range(self.num_buckets):
            self.buckets.append(
                defaultdict(lambda: {"requests": 0, "errors": 0, "latencies": []})
            )

        # track absolute bucket start timestamps for expiry
        self.bucket_start_ts: deque = deque(maxlen=self.num_buckets)

        """
        This block (below) is about tracking the start times of each time bucket in your sliding window aggregator
        """
        now = datetime.utcnow().timestamp()
        base = int(now // bucket_Seconds) * bucket_Seconds
        for i in range(self.num_buckets):
            self.bucket_start_ts.append(
                base - (self.num_buckets - 1 - i) * self.bucket_seconds
            )

        def _current_bucket_index(self, ts: float) -> int:
            """
            Map timestamp to bucket index (0..num_buckets-1), advancing buckets if necessary.

            Args:
                ts: Unix timestamp in seconds.

            Returns:
                index of bucket to write into.
            """

            if ts < self.bucket_start_ts[0]:
                return -1
            last_bucket_start = self.bucket_start_ts[-1]  # right side of the window
            if ts >= last_bucket_start + self.bucket_seconds:
                shift = int((ts - last_bucket_start) // self.bucket_seconds)
                for _ in range(min(shift, self.num_buckets)):
                    self.buckets.append(
                        defaultdict(
                            lambda: {"requests": 0, "errors": 0, "latencies": []}
                        )
                    )
                    self.bucket_start_ts.append(
                        self.bucket_start_ts[-1] + self.bucket_seconds
                    )
            # index calculation of the log arrived to where the log should go in like in which bucket the log should go in
            idx = (
                len(self.bucket_start_ts)
                - 1
                - int(
                    (self.bucket_start_ts[-1] + self.bucket_seconds - ts)
                    // self.bucket_seconds
                )
            )
            # clamp
            idx = max(0, min(len(self.bucket_start_ts) - 1, idx))
            return idx

        def add(
            self,
            ts: float,
            service: str,
            endpoint: str,
            is_error: bool,
            latency_ms: float,
        ) -> None:
            """
            Add an event to the appropriate bucket.

            Args:
                ts: Timestamp of the log (Unix time, float).
                service: Name of the service (e.g., "webapp").
                endpoint: API endpoint (e.g., "/login").
                status: HTTP status code (e.g., 200, 500).
                latency_ms: Latency of the request in ms.
            """
            idx = self._current_bucket_index(ts)
            if idx < 0:
                return
            key = (service, endpoint)
            bucket = self.buckets[idx][key]
            bucket["requests"] += 1
            if is_error:
                bucket["errors"] += 1
            bucket["latencies"].append(latency_ms)

        # If add() is about storing logs into buckets, then snapshot() is about READING the current state of the sliding window and turning it into useful metrics.

        def snapshot(self) -> Dict[Tuple[str, str], Dict[str, float]]:
            """
            Compute aggregated metrics across the window.

            Returns:
                dict keyed by (service, endpoint) with aggregated metrics:
                    - requests: total requests in window
                    - errors: total errors in window
                    - error_rate: errors / requests or 0
                    - avg_latency_ms
            """
            results: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(
                lambda: {"requests": 0, "errors": 0, "latencies": []}
            )
            # aggregate across all buckets
            for bucket in self.buckets:
                for key, stats in bucket.items():
                    results[key]["requests"] += stats["requests"]
                    results[key]["errors"] += stats["errors"]
                    results[key]["latencies"].extend(stats["latencies"])

            # finalize averages and error rates
            for key, stats in results.items():
                reqs = stats["requests"]
                errs = stats["errors"]
                lats = stats["latencies"]

                stats["error_rate"] = errs / reqs if reqs > 0 else 0.0
                stats["avg_latency"] = sum(lats) / len(lats) if lats else 0.0
                del stats["latencies"]  # remove raw latencies to keep snapshot compact

            return results

