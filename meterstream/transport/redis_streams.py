"""Redis Streams broker, partitioned by meter.

Detectors keep per-meter state (baselines, last register values), so all readings for a meter
must reach the same worker. Readings are routed to one of N streams by crc32(meter_id) % N (like
Kafka partitions); each worker owns one partition. Scaling out = more partitions and workers.

Consumer groups give at-least-once delivery: a message stays in the group's pending list until a
worker XACKs it. If a worker dies mid-batch, XAUTOCLAIM hands its pending messages to a live
worker after `claim_idle_ms`. Duplicates are therefore possible and the store dedupes on
(meter_id, ts). The stream is capped with MAXLEN ~ so Redis memory stays bounded.
"""

from __future__ import annotations

import zlib

from meterstream.transport import GROUP, STREAM


class RedisBroker:
    def __init__(
        self,
        url: str,
        stream: str = STREAM,
        group: str = GROUP,
        maxlen: int = 1_000_000,
        claim_idle_ms: int = 60_000,
        partitions: int = 1,
        partition: int = 0,
    ):
        import redis  # lazy: tests and the memory broker don't need it

        self.r = redis.Redis.from_url(url, decode_responses=True)
        self.base, self.group, self.maxlen, self.claim_idle_ms = stream, group, maxlen, claim_idle_ms
        self.partitions = partitions
        self.streams = [f"{stream}:{i}" for i in range(partitions)] if partitions > 1 else [stream]
        self.stream = self.streams[partition]  # the partition this instance consumes
        for s in self.streams:
            try:
                self.r.xgroup_create(s, group, id="0", mkstream=True)
            except redis.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    def partition_for(self, meter_id: str) -> str:
        return self.streams[zlib.crc32(meter_id.encode()) % self.partitions]

    def publish(self, messages: list[dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for m in messages:
            target = self.partition_for(str(m.get("meter_id", "")))
            pipe.xadd(target, {k: str(v) for k, v in m.items()}, maxlen=self.maxlen, approximate=True)
        pipe.execute()

    def consume(self, consumer: str, count: int = 500, block_ms: int = 1000) -> list[tuple[str, dict]]:
        # First take over messages abandoned by dead consumers, then read new ones.
        _, claimed, _ = self.r.xautoclaim(self.stream, self.group, consumer, self.claim_idle_ms, "0-0", count=count)
        if claimed:
            return [(i, f) for i, f in claimed if f]
        resp = self.r.xreadgroup(self.group, consumer, {self.stream: ">"}, count=count, block=block_ms)
        return [(i, f) for _, entries in resp for i, f in entries] if resp else []

    def ack(self, ids: list[str]) -> None:
        if ids:
            self.r.xack(self.stream, self.group, *ids)

    def lag(self) -> int:
        info = self.r.xinfo_groups(self.stream)
        g = next((g for g in info if g["name"] == self.group), None)
        if not g:
            return 0
        return int(g.get("lag") or 0) + int(g.get("pending") or 0)

    # Asset registry: which meters have generation (solar). In production this would come from
    # the utility's asset database; here the simulator publishes it.
    REGISTRY_KEY = "registry:solar_meters"

    def save_solar_registry(self, meter_ids: set[str]) -> None:
        pipe = self.r.pipeline()
        pipe.delete(self.REGISTRY_KEY)
        if meter_ids:
            pipe.sadd(self.REGISTRY_KEY, *meter_ids)
        pipe.execute()

    def solar_registry(self) -> set[str]:
        return set(self.r.smembers(self.REGISTRY_KEY))
