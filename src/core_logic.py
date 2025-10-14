"""
core_logic.py

Contains the Ray remote functions and Actors:

- parse_logs_remote: stateless parser that runs in parallel.
- AggregatorActor: stateful actor using SlidingWindowAggregator to maintain metrics.
- AnomalyDetectorActor: inspects aggregator state and triggers alerts.
- AlertingActor: receives alerts and keeps an in-memory history (could forward externally).
"""

from typing import List, Dict, Any, Optional
import ray
import logging
import ujson as json  # faster JSON; used for potential serialization
from datetime import datetime, timezone
from time import time
from src.utils import parse_log_line, SlidingWindowAggregator, LogLine, DEFAULT_CONFIG

logger = logging.getLogger("core_logic")


@ray.remote
def parse_logs_remote(
    raw_batch: List[str], start: Optional[int] = None, end: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Stateless remote function that parses raw log lines into structured dicts.

    Args:
        raw_batch: List of raw log line strings (the entire batch may be passed as an object ref).
        start: Start index in raw_batch to parse (inclusive).
        end: End index in raw_batch to parse (exclusive). If None, parse to end.

    Returns:
        A list of dicts for successfully parsed lines (each dict derived from LogLine.to_dict()).

    Notes:
        - This function is intentionally stateless and suitable for horizontal scaling.
        - It reads from the provided list and returns a (possibly smaller) list of parsed items.
    """
    try:
        sl = raw_batch if start is None and end is None else raw_batch[start:end]
        parsed = []
        for line in sl:
            obj = parse_log_line(line)
            if obj is None:
                continue
            parsed.append(obj.to_dict())
        # Return parsed list (Ray will keep it in object store)
        return parsed
    except Exception as e:
        logger.exception("parse_logs_remote failed: %s", e)
        # Return empty list on failure but don't crash the caller
        return []


@ray.remote
class AggregatorActor:
    """
    Stateful actor that maintains sliding-window aggregates using SlidingWindowAggregator.

    Methods:
        ingest_parsed_batch(parsed_ref_or_list): Accepts either a direct list of dicts or an objectref.
        get_snapshot(): returns current aggregated snapshot.
        get_top_k(k): returns top-k endpoints by request volume.
    """

    def __init__(
        self,
        window_seconds: int = DEFAULT_CONFIG["detector"]["window_seconds"],
        bucket_seconds: int = DEFAULT_CONFIG["detector"]["bucket_seconds"],
    ) -> None:
        """
        Initialize the actor and internal SlidingWindowAggregator.

        Args:
            window_seconds: Sliding window size in seconds.
            bucket_seconds: Bucket granularity in seconds.
        """
        self._aggregator = SlidingWindowAggregator(
            window_seconds=window_seconds, bucket_seconds=bucket_seconds
        )
        self._total_counts = (
            {}
        )  # persistent totals across lifetime keyed by (service, endpoint)
        self._last_update_ts = datetime.now(timezone.utc).timestamp()

    def _process_parsed(self, parsed: List[Dict[str, Any]]) -> None:
        """
        Internal helper to process parsed records.

        Args:
            parsed: list of dicts (from parse_logs_remote).
        """
        for rec in parsed:
            try:
                ts = float(
                    rec.get(
                        "timestamp", rec.get("timestamp", datetime.now().timestamp())
                    )
                )
                service = rec.get("service", "unknown")
                endpoint = rec.get("endpoint", "unknown")
                status = int(rec.get("status", 0))
                latency = float(rec.get("latency_ms") or rec.get("latency", 0.0))
                is_error = status >= 500
                self._aggregator.add(ts, service, endpoint, is_error, latency)
                key = (service, endpoint)
                if key not in self._total_counts:
                    self._total_counts[key] = {"requests": 0, "errors": 0}
                self._total_counts[key]["requests"] += 1
                if is_error:
                    self._total_counts[key]["errors"] += 1
            except Exception:
                logger.exception("Failed to add parsed record: %s", rec)

    def ingest_parsed_batch(self, parsed_batch: Any) -> None:
        """
        Ingest a batch of parsed logs.

        Args:
            parsed_batch: Either a list of dicts or a Ray object ref to such a list.

        Returns:
            None
        """
        # Support object refs by checking for ray.ObjectRef type
        try:
            # lazy import to avoid heavy dependency at module import time
            from ray import ObjectRef

            if isinstance(parsed_batch, ObjectRef):
                parsed = ray.get(parsed_batch)
            else:
                parsed = parsed_batch
        except Exception:
            # fallback if ObjectRef is not available in runtime or input is not a ref
            try:
                parsed = (
                    ray.get(parsed_batch)
                    if hasattr(parsed_batch, "hex")
                    or hasattr(parsed_batch, "__ray_object__")
                    else parsed_batch
                )
            except Exception:
                parsed = parsed_batch
        if not isinstance(parsed, list):
            logger.warning("ingest_parsed_batch received non-list: %s", type(parsed))
            return
        self._process_parsed(parsed)
        self._last_update_ts = datetime.now(timezone.utc).timestamp()

    def get_snapshot(self) -> Dict[str, Any]:
        """
        Retrieve aggregated snapshot across sliding window and totals.

        Returns:
            Dict with 'window' and 'totals' keys.
        """
        window_stats = self._aggregator.snapshot()
        totals = {f"{k[0]}|{k[1]}": v for k, v in self._total_counts.items()}
        return {
            "window": window_stats,
            "totals": totals,
            "last_update_ts": self._last_update_ts,
        }

    def get_top_k(self, k: int = 10) -> List[Dict[str, Any]]:
        """
        Return top-k endpoints by requests in the sliding window.

        Args:
            k: Number of top entries.

        Returns:
            List of dicts {service, endpoint, requests, errors, error_rate, avg_latency_ms}
        """
        snap = self._aggregator.snapshot()
        items = []
        for (service, endpoint), m in snap.items():
            items.append(
                {
                    "service": service,
                    "endpoint": endpoint,
                    "requests": m["requests"],
                    "errors": m["errors"],
                    "error_rate": m["error_rate"],
                    "avg_latency_ms": m["avg_latency_ms"],
                }
            )
        items.sort(key=lambda x: x["requests"], reverse=True)
        return items[:k]


@ray.remote
class AlertingActor:
    """
    Simple alerting actor storing recent alerts. Intended to be replaced by real alert sinks.

    Methods:
        alert(alert_payload): receive a dict describing the alert.
        get_alerts(limit): return last `limit` alerts.
    """

    def __init__(self, max_alerts: int = 1000) -> None:
        """
        Initialize alert storage.

        Args:
            max_alerts: maximum number of alerts to retain in memory.
        """
        self._alerts: List[Dict[str, Any]] = []
        self._max_alerts = max_alerts

    def alert(self, payload: Dict[str, Any]) -> None:
        """
        Receive an alert.

        Args:
            payload: dictionary describing the alert. Expected keys: message, ts, meta.

        Returns:
            None
        """
        payload["ts"] = payload.get("ts", datetime.now(timezone.utc).isoformat())
        self._alerts.append(payload)
        if len(self._alerts) > self._max_alerts:
            self._alerts.pop(0)
        # In a real system, we'd push to Slack / PagerDuty / etc. Here we simply log.
        logger.warning("ALERT raised: %s", payload)

    def get_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Return last `limit` alerts, newest first.

        Args:
            limit: maximum number of alerts to return.

        Returns:
            List of alert dicts.
        """
        return list(reversed(self._alerts[-limit:]))


@ray.remote
class AnomalyDetectorActor:
    """
    Actor that inspects aggregator metrics and decides whether to raise alerts.

    Periodically called (by driver) via `scan_and_alert`.
    """

    def __init__(
        self,
        aggregator: ray.actor.ActorHandle,
        alerting: ray.actor.ActorHandle,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize detector.

        Args:
            aggregator: Actor handle to AggregatorActor.
            alerting: Actor handle to AlertingActor.
            config: Dict of thresholds and windowing parameters.
        """
        self.aggregator = aggregator
        self.alerting = alerting
        self.config = config or DEFAULT_CONFIG["detector"]
        self._last_seen: Dict[str, float] = (
            {}
        )  # track last alert times per key to avoid spamming

    def _should_alert(self, key: str, metrics: Dict[str, Any]) -> bool:
        """
        Decide whether to alert for a given key.

        Args:
            key: string key (service|endpoint).
            metrics: dict containing 'requests', 'errors', 'error_rate', 'avg_latency_ms'.

        Returns:
            True if alert condition met.
        """
        min_reqs = self.config.get("min_requests_threshold", 100)
        err_th = self.config.get("error_rate_threshold", 0.10)
        if metrics["requests"] < min_reqs:
            return False
        if metrics["error_rate"] >= err_th:
            # simple cooldown: don't alert same key more than once per window
            now = time()
            last_ts = self._last_seen.get(key, 0.0)
            if now - last_ts > self.config.get("window_seconds", 60):
                self._last_seen[key] = now
                return True
        return False

    def scan_and_alert(self) -> None:
        """
        Pull snapshot from aggregator and evaluate alerts.

        Returns:
            None
        """
        try:
            snapshot = ray.get(self.aggregator.get_snapshot.remote())
            window = snapshot.get("window", {})
            for (service, endpoint), metrics in window.items():
                key = f"{service}|{endpoint}"
                if self._should_alert(key, metrics):
                    alert_payload = {
                        "message": "High error rate detected",
                        "service": service,
                        "endpoint": endpoint,
                        "metrics": metrics,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    }
                    # send to alerting actor
                    self.alerting.alert.remote(alert_payload)
        except Exception as e:
            logger.exception("scan_and_alert failed: %s", e)
