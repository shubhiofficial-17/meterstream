from meterstream.detectors import OutageDetector, SpikeDetector, TamperDetector, VoltageDetector
from meterstream.models import AlertKind, Reading

T0 = 1_767_225_600


def rd(ts, kw=0.5, v=230.0, imp=100.0, exp=0.0, meter="M1", seq=None):
    return Reading(meter, ts, seq if seq is not None else ts // 300, imp, exp, kw, v)


def test_spike_needs_baseline_then_fires_on_large_jump():
    d = SpikeDetector(warmup=3)
    for day in range(5):  # same hour on 5 days
        assert d.observe(rd(T0 + day * 86_400, kw=0.4)) == []
    alerts = d.observe(rd(T0 + 5 * 86_400, kw=4.0))
    assert [a.kind for a in alerts] == [AlertKind.SPIKE]


def test_spike_respects_time_of_day():
    d = SpikeDetector(warmup=3)
    evening = T0 + 19 * 3600
    for day in range(5):
        d.observe(rd(T0 + day * 86_400 + 3 * 3600, kw=0.3))  # 3 am: low
        d.observe(rd(evening + day * 86_400, kw=3.0))  # 7 pm: high
    assert d.observe(rd(evening + 5 * 86_400, kw=3.2)) == []  # normal for the evening
    assert d.observe(rd(T0 + 5 * 86_400 + 3 * 3600, kw=3.2))  # abnormal at 3 am


def test_spike_does_not_learn_from_anomalies():
    d = SpikeDetector(warmup=3)
    for day in range(5):
        d.observe(rd(T0 + day * 86_400, kw=0.4))
    for day in range(5, 10):  # a run of spikes keeps alerting
        assert d.observe(rd(T0 + day * 86_400, kw=5.0))


def test_spike_skips_solar_meters_in_daylight():
    d = SpikeDetector(warmup=1, solar_meters={"M1"})
    noon = T0 + 12 * 3600
    for day in range(3):
        d.observe(rd(noon + day * 86_400, kw=0.01))
    assert d.observe(rd(noon + 3 * 86_400, kw=3.0)) == []


def test_register_rollback_and_reverse_flow():
    d = TamperDetector(solar_meters={"SOLAR"})
    d.observe(rd(T0, imp=500.0))
    assert [a.kind for a in d.observe(rd(T0 + 300, imp=380.0))] == [AlertKind.REGISTER_ROLLBACK]
    d.observe(rd(T0, exp=0.0, meter="M2"))
    assert [a.kind for a in d.observe(rd(T0 + 300, exp=0.2, meter="M2"))] == [AlertKind.REVERSE_FLOW]
    d.observe(rd(T0, exp=0.0, meter="SOLAR"))
    assert d.observe(rd(T0 + 300, exp=0.5, meter="SOLAR")) == []  # solar export is legitimate


def test_voltage_limits():
    d = VoltageDetector()
    assert d.observe(rd(T0, v=229.0)) == []
    assert d.observe(rd(T0, v=190.0))[0].kind == AlertKind.VOLTAGE_SAG
    assert d.observe(rd(T0, v=260.0))[0].kind == AlertKind.VOLTAGE_SWELL


def test_outage_sweep_and_restore():
    d = OutageDetector(interval_s=300, missed_intervals=3)
    d.observe(rd(T0))
    assert d.sweep(T0 + 900) == []  # 2 missed intervals: not yet
    swept = d.sweep(T0 + 1200)
    assert [a.kind for a in swept] == [AlertKind.OUTAGE]
    assert d.sweep(T0 + 1500) == []  # reported once
    restored = d.observe(rd(T0 + 3000))
    assert restored and "restored" in restored[0].detail and restored[0].ts == T0 + 300
