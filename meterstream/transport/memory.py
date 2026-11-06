"""In-process broker with consumer-group semantics (pending until acked). For tests and benchmarks."""

from __future__ import annotations

from collections import OrderedDict, deque


class MemoryBroker:
    def __init__(self) -> None:
        self._queue: deque[tuple[str, dict]] = deque()
        self._pending: OrderedDict[str, dict] = OrderedDict()
        self._next_id = 0
        self._solar: set[str] = set()

    def publish(self, messages: list[dict]) -> None:
        for m in messages:
            self._next_id += 1
            self._queue.append((f"{self._next_id}-0", m))

    def consume(self, consumer: str, count: int = 500, block_ms: int = 1000) -> list[tuple[str, dict]]:
        batch = []
        while self._queue and len(batch) < count:
            msg_id, msg = self._queue.popleft()
            self._pending[msg_id] = msg
            batch.append((msg_id, msg))
        return batch

    def ack(self, ids: list[str]) -> None:
        for i in ids:
            self._pending.pop(i, None)

    def redeliver_pending(self) -> int:
        """Simulate a crashed consumer: put unacked messages back on the queue."""
        n = len(self._pending)
        for msg_id, msg in reversed(self._pending.items()):
            self._queue.appendleft((msg_id, msg))
        self._pending.clear()
        return n

    def lag(self) -> int:
        return len(self._queue) + len(self._pending)

    def save_solar_registry(self, meter_ids: set[str]) -> None:
        self._solar = set(meter_ids)

    def solar_registry(self) -> set[str]:
        return set(self._solar)
