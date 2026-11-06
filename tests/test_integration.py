"""Integration tests against real Redis and TimescaleDB. Run in CI; skipped locally unless the URLs are set."""

import os
import uuid

import pytest

from meterstream.metrics import Metrics
from meterstream.pipeline import Pipeline
from meterstream.simulator import FleetSimulator, SimConfig

REDIS_URL = os.environ.get("REDIS_TEST_URL")
DB_URL = os.environ.get("TIMESCALE_TEST_URL")


@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_TEST_URL")
def test_redis_streams_at_least_once():
    from meterstream.transport.redis_streams import RedisBroker

    stream = f"test-{uuid.uuid4().hex[:8]}"
    b = RedisBroker(REDIS_URL, stream=stream, claim_idle_ms=0)
    try:
        b.publish([{"meter_id": "M1", "n": i} for i in range(10)])
        first = b.consume("dies", count=10, block_ms=100)
        assert len(first) == 10  # consumer "dies" without acking
        reclaimed = b.consume("alive", count=10, block_ms=100)  # XAUTOCLAIM hands them over
        assert [m["n"] for _, m in reclaimed] == [str(i) for i in range(10)]
        b.ack([i for i, _ in reclaimed])
        assert b.lag() == 0
    finally:
        b.r.delete(stream)


@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_TEST_URL")
def test_redis_partitions_keep_each_meter_on_one_stream():
    from meterstream.transport.redis_streams import RedisBroker

    base = f"test-{uuid.uuid4().hex[:8]}"
    brokers = [RedisBroker(REDIS_URL, stream=base, partitions=3, partition=i) for i in range(3)]
    try:
        brokers[0].publish([{"meter_id": f"M{m}", "n": n} for n in range(5) for m in range(30)])
        seen: dict[str, set[int]] = {}
        for i, b in enumerate(brokers):
            for _, msg in b.consume(f"w{i}", count=1000, block_ms=100):
                seen.setdefault(msg["meter_id"], set()).add(i)
        assert len(seen) == 30 and all(len(p) == 1 for p in seen.values())
    finally:
        brokers[0].r.delete(*brokers[0].streams)


@pytest.mark.skipif(not DB_URL, reason="set TIMESCALE_TEST_URL")
def test_timescale_store_end_to_end():
    from meterstream.storage.timescale import TimescaleStore

    store = TimescaleStore(DB_URL)
    sim = FleetSimulator(SimConfig(meters=10, seed=11, anomaly_rate=0.01))
    readings = list(sim.run(2 * 288))
    p = Pipeline(store, metrics=Metrics())
    for i in range(0, len(readings), 500):
        p.process([r.to_dict() for r in readings[i : i + 500]])
    unique = len({(r.meter_id, r.ts) for r in readings})
    assert store.fleet_summary()["readings"] >= unique
    assert store.write_readings(readings[:100]) == 0  # duplicates ignored
    rows = store.readings("MTR-000000", readings[0].ts, readings[-1].ts + 1, bucket_s=3600)
    assert 40 <= len(rows) <= 49 and all(r["samples"] <= 12 for r in rows)
    assert store.incidents(limit=5)
