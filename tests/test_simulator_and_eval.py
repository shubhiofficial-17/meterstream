from eval.evaluate_detectors import main
from meterstream.metrics import Metrics
from meterstream.simulator import FleetSimulator, SimConfig, load_shape


def test_simulator_is_deterministic():
    a = list(FleetSimulator(SimConfig(meters=5, seed=9)).run(400))
    b = list(FleetSimulator(SimConfig(meters=5, seed=9)).run(400))
    assert a == b


def test_registers_are_monotonic_without_tamper():
    sim = FleetSimulator(SimConfig(meters=20, seed=2, anomaly_rate=0))
    last = {}
    for r in sim.run(600):
        assert r.energy_import_kwh >= last.get(r.meter_id, 0)
        last[r.meter_id] = r.energy_import_kwh
    assert sim.labels == []


def test_load_shape_has_evening_peak():
    assert load_shape(19.5) > load_shape(12) > load_shape(3)


def test_metrics_render():
    m = Metrics()
    m.inc("x_total", 2, "help", kind="a")
    m.set("y", 1.5, "gauge")
    text = m.render()
    assert 'x_total{kind="a"} 2.0' in text and "# TYPE y gauge" in text


def test_detector_quality_gate():
    assert main(["--meters", "80", "--days", "4", "--min-f1", "0.9"]) == 0
