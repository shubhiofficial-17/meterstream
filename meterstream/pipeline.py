"""Ingest pipeline: consume -> validate -> dedupe -> detect -> store -> ack.

Messages are acknowledged only after readings and incidents are written, so a crash anywhere
before that just means redelivery (at-least-once). Redelivered readings are harmless: the store
ignores duplicate (meter_id, ts) and detectors skip readings they have already seen.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from meterstream.detectors import OutageDetector, SpikeDetector, TamperDetector, VoltageDetector
from meterstream.incidents import IncidentTracker
from meterstream.metrics import Metrics
from meterstream.metrics import metrics as default_metrics
from meterstream.models import Alert, AlertKind, Incident, Reading
from meterstream.storage import Store
from meterstream.transport import Broker

log = logging.getLogger("meterstream")


@dataclass
class Pipeline:
    store: Store
    interval_s: int = 300
    solar_meters: set[str] = field(default_factory=set)
    metrics: Metrics = field(default_factory=lambda: default_metrics)

    def __post_init__(self) -> None:
        self.spike = SpikeDetector(solar_meters=self.solar_meters)
        self.tamper = TamperDetector(solar_meters=self.solar_meters)
        self.voltage = VoltageDetector()
        self.outage = OutageDetector(interval_s=self.interval_s)
        self.tracker = IncidentTracker(interval_s=self.interval_s)
        self._last_ts: dict[str, int] = {}
        self.watermark = 0  # highest event time seen; housekeeping runs on event time, not wall time
        self.alerts: list[Alert] = []  # kept for evaluation; bounded by caller in production use

    def process(self, raw: list[dict]) -> dict:
        """Process one batch of raw messages. Returns counts for logging/metrics."""
        readings, invalid = [], 0
        for msg in raw:
            try:
                readings.append(Reading.from_dict(msg))
            except (KeyError, ValueError, TypeError):
                invalid += 1
        readings.sort(key=lambda r: (r.ts, r.meter_id))

        changed: dict[str, Incident] = {}
        fresh: list[Reading] = []
        for r in readings:
            if r.ts <= self._last_ts.get(r.meter_id, 0):
                continue  # duplicate or out-of-order replay: already processed
            self._last_ts[r.meter_id] = r.ts
            self.watermark = max(self.watermark, r.ts)
            fresh.append(r)
            for alert in self._detect(r):
                inc, _ = self.tracker.add(alert)
                if alert.kind == AlertKind.OUTAGE:
                    inc.end_ts = r.ts - self.interval_s  # last silent interval
                    self.tracker.resolve(r.meter_id, AlertKind.OUTAGE)
                changed[inc.id] = inc

        inserted = self.store.write_readings(fresh)
        if changed:
            self.store.upsert_incidents(list(changed.values()))

        m = self.metrics
        m.inc("meterstream_readings_total", len(fresh), "Readings processed")
        m.inc("meterstream_duplicates_total", len(readings) - len(fresh), "Duplicate/replayed readings skipped")
        m.inc("meterstream_invalid_total", invalid, "Messages that failed validation")
        return {
            "received": len(raw),
            "processed": len(fresh),
            "inserted": inserted,
            "invalid": invalid,
            "incidents_changed": len(changed),
        }

    def _detect(self, r: Reading) -> list[Alert]:
        alerts = [*self.outage.observe(r), *self.tamper.observe(r), *self.voltage.observe(r), *self.spike.observe(r)]
        alerts = [replace(a, detected_ts=r.ts) for a in alerts]
        for a in alerts:
            self.metrics.inc("meterstream_alerts_total", 1, "Alerts raised", kind=a.kind.value)
        self.alerts.extend(alerts)
        return alerts

    def tick(self, now: int) -> list[Incident]:
        """Periodic housekeeping: detect silent meters and close stale incidents."""
        changed = []
        for alert in (replace(a, detected_ts=now) for a in self.outage.sweep(now)):
            self.metrics.inc("meterstream_alerts_total", 1, "Alerts raised", kind=alert.kind.value)
            self.alerts.append(alert)
            inc, _ = self.tracker.add(alert)
            changed.append(inc)
        changed += self.tracker.close_stale(now)
        if changed:
            self.store.upsert_incidents(changed)
        return changed


def run_worker(
    broker: Broker,
    pipeline: Pipeline,
    consumer: str,
    batch: int = 1000,
    tick_every_s: float = 30.0,
    stop: Callable[[], bool] = lambda: False,
    on_tick: Callable[[], None] | None = None,
) -> None:
    """Long-running consumer loop for one worker process."""
    if on_tick:
        on_tick()
    last_tick = time.monotonic()
    while not stop():
        msgs = broker.consume(consumer, count=batch, block_ms=1000)
        if msgs:
            started = time.perf_counter()
            stats = pipeline.process([m for _, m in msgs])
            broker.ack([i for i, _ in msgs])
            pipeline.metrics.set("meterstream_batch_seconds", time.perf_counter() - started, "Last batch latency")
            log.debug("batch %s", stats)
        if time.monotonic() - last_tick >= tick_every_s and pipeline.watermark:
            # Event time, not wall-clock time: replaying a backlog must not flag every meter as silent.
            if on_tick:
                on_tick()
            pipeline.tick(pipeline.watermark + pipeline.interval_s)
            pipeline.metrics.set("meterstream_consumer_lag", broker.lag(), "Unacked messages in the stream")
            last_tick = time.monotonic()
