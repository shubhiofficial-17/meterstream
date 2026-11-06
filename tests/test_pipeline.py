from meterstream.metrics import Metrics
from meterstream.models import AlertKind
from meterstream.pipeline import Pipeline
from meterstream.simulator import FleetSimulator, SimConfig
from meterstream.storage.memory import MemoryStore
from meterstream.transport.memory import MemoryBroker


def _pipe(**kw):
    return Pipeline(MemoryStore(), metrics=Metrics(), **kw)


def test_invalid_messages_are_counted_not_fatal():
    p = _pipe()
    good = {
        "meter_id": "M1",
        "ts": 1_767_225_600,
        "seq": 1,
        "energy_import_kwh": 1,
        "energy_export_kwh": 0,
        "power_kw": 0.4,
        "voltage_v": 231,
    }
    stats = p.process([good, {"meter_id": "M1"}, {**good, "ts": 1_767_225_900, "voltage_v": -5}])
    assert stats["processed"] == 1 and stats["invalid"] == 2


def test_duplicates_are_skipped_and_values_parsed_from_strings():
    p = _pipe()
    msg = {
        "meter_id": "M1",
        "ts": "1767225600",
        "seq": "1",
        "energy_import_kwh": "10.5",
        "energy_export_kwh": "0",
        "power_kw": "0.4",
        "voltage_v": "231.2",
    }  # how Redis returns fields
    assert p.process([msg, msg])["processed"] == 1
    assert p.process([msg])["processed"] == 0
    assert p.metrics.get("meterstream_duplicates_total") == 2


def test_crash_before_ack_is_redelivered_without_double_counting():
    sim = FleetSimulator(SimConfig(meters=20, seed=1, duplicate_rate=0))
    broker, store = MemoryBroker(), MemoryStore()
    broker.publish([r.to_dict() for r in sim.run(50)])
    p = Pipeline(store, metrics=Metrics())
    batch = broker.consume("w1", count=300)
    p.process([m for _, m in batch])  # worker "crashes" here, before ack
    assert broker.redeliver_pending() == 300

    p2 = Pipeline(store, metrics=Metrics())  # a fresh worker with no in-memory state
    while msgs := broker.consume("w2", count=500):
        p2.process([m for _, m in msgs])
        broker.ack([i for i, _ in msgs])
    assert broker.lag() == 0
    assert store.fleet_summary()["readings"] == 20 * 50  # every reading stored exactly once


def test_end_to_end_detects_injected_tamper():
    cfg = SimConfig(meters=50, seed=3, anomaly_rate=0.01, kinds=(AlertKind.REGISTER_ROLLBACK,))
    sim = FleetSimulator(cfg)
    p = _pipe(interval_s=cfg.interval_s)
    for r in sim.run(2 * 288):
        p.process([r.to_dict()])
    incs = p.store.incidents(kind="register_rollback", limit=1000)
    assert sim.labels
    for lb in sim.labels:  # every injected rollback is covered (rollbacks minutes apart merge into one incident)
        assert any(i["meter_id"] == lb.meter_id and i["start_ts"] <= lb.start_ts <= i["end_ts"] for i in incs)


def test_incidents_merge_consecutive_alerts():
    cfg = SimConfig(meters=30, seed=5, anomaly_rate=0.02, kinds=(AlertKind.REVERSE_FLOW,))
    sim = FleetSimulator(cfg)
    solar = {m.profile.meter_id for m in sim.meters if m.profile.has_solar}
    p = _pipe(interval_s=cfg.interval_s, solar_meters=solar)
    for r in sim.run(2 * 288):
        p.process([r.to_dict()])
    incs = p.store.incidents(kind="reverse_flow", limit=1000)
    assert incs and len(incs) <= len(sim.labels)
    assert max(i["alerts"] for i in incs) > 1  # multi-interval anomalies become one incident
