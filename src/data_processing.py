"""
data_processing.py

Simulated log generator and batching helpers.

Produces log lines in format:
  2025-09-20T12:34:56.789Z | webapp | /api/v1/items | status=200 | lat=12.34ms | user=u-12345

Functions:
- LogGenerator: yields log lines with configurable distributions.
- batch_logs: yields batches (lists) of lines for ingestion.
"""

from typing import Iterator, List, Optional
import random
from datetime import datetime, timezone
import time
from itertools import islice
import logging


logger = logging.getLogger("data_processing")


class LogGenerator:
    """
    Simple probabilistic log generator to simulate traffic.

    Supports multiple services, endpoints, and user distributions. Introduces
    occasional spikes/errors for anomaly simulation.
    """

    # -------------------------------
    # 3. The * (asterisk) in __init__
    # -------------------------------

    # def __init__(self, *, services: ..., endpoints: ..., ...):
    """
        The * (asterisk) here is called a 'keyword-only argument separator'.

        Meaning:
        - After the * symbol, all parameters must be passed by NAME, not by position.
        - This improves code readability and prevents accidental argument misplacement.
    """

    # ❌ Not allowed (positional arguments after *)
    # LogGenerator(["auth"], ["/login"])

    # ✅ Correct (keyword-only arguments)
    # LogGenerator(services=["auth"], endpoints=["/login"])

    # 👉 In short:
    # The * forces you to use named arguments like "services=" and "endpoints=",
    # making your code safer, clearer, and easier to read.

    def __init__(
        self,
        *,
        services: Optional[list[str]] = None,
        endpoints: Optional[list[str]] = None,
        seed: Optional[int] = None,
        error_rate: float = 0.02,
        spike_probability: float = 0.05,
    ) -> None:
        """
        Initialize generator.

        Args:
            services: List of service names.
            endpoints: List of endpoints.
            seed: Random seed for reproducible results.
            error_rate: Base probability of generating a server error (5xx).
            spike_probability: Probability per line to be part of a spike (higher error rate).
        """
        if seed is not None:
            random.seed(seed)
            self.services = services or ["webapp", "auth", "payments", "search"]
            self.endpoints = endpoints or [
                "/",
                "/api/v1/items",
                "/api/v1/items/{id}",
                "/api/v1/search",
                "/login",
                "/checkout",
            ]
            self.error_rate = error_rate
            self.spike_probability = spike_probability
            self.user_counter = 0

        def _now_iso(self) -> str:
            return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _random_endpoint(self) -> str:
        """Return endpoint pattern, possibly with id substituted."""
        ep = random.choice(self.endpoints)
        if "{id}" in ep:
            return ep.replace("{id}", str(random.randint(1, 10000)))
        return ep

    def _random_status(self, in_spike: bool = False) -> int:
        """Return HTTP status."""
        base_error = self.error_rate * (5 if in_spike else 1)
        r = random.random()
        if r < base_error:
            # prefer 500 > 502 etc
            return random.choice([500, 502, 503, 504])
        # client errors are less common
        if r < base_error + 0.02:
            return random.choice([400, 401, 403, 404])
        return 200

    def _random_latency(self) -> float:
        """Simulate latency in ms (float)."""
        # mixture of typical and occasional heavy tail
        if random.random() < 0.95:
            return max(1.0, random.gauss(60, 30))
        else:
            return max(10.0, random.gauss(600, 300))

    def _random_user(self) -> str:
        """Generate a user id."""
        self.user_counter += 1
        return f"u-{self.user_counter % 10000}"

    def generate(self, count: int = 1000) -> Iterator[str]:
        """
        Generate `count` log lines.

        Args:
            count: Number of lines to produce.

        Yields:
            Raw log line strings.
        """
        for _ in range(count):
            in_spike = random.random() < self.spike_probability
            ts = self._now_iso()
            service = random.choice(self.services)
            endpoint = self._random_endpoint()
            status = self._random_status(in_spike=in_spike)
            lat = self._random_latency()
            user = self._random_user()
            line = f"{ts} | {service} | {endpoint} | status={status} | lat={lat:.2f}ms | user={user}"
            yield line

    def stream_forever(self, batch_size: int = 1000) -> Iterator[List[str]]:
        """
        Infinite generator yielding batches of log lines.

        Args:
            batch_size: Number of lines per yielded batch.
        """
        while True:
            yield list(self.generate(batch_size))
            # In real ingestion, this would block on new data arrival; we emulate slight delay
            time.sleep(0.01)


def batch_logs(
    generator: LogGenerator, num_batches: int = 10, lines_per_batch: int = 1000
) -> Iterator[List[str]]:
    """
    Convenience to produce `num_batches` batches of logs.

    Args:
        generator: LogGenerator instance.
        num_batches: How many batches to produce.
        lines_per_batch: Number of lines per batch.

    Yields:
        Lists of raw log lines.
    """
    for _ in range(num_batches):
        batch = list(generator.generate(lines_per_batch))
        yield batch
