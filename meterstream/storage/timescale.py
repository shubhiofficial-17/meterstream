"""TimescaleDB store.

Readings live in a hypertable partitioned by time (1-day chunks), with:
  * a primary key on (meter_id, ts) so redelivered messages are dropped by ON CONFLICT DO NOTHING
  * a 15-minute continuous aggregate that the API reads for charts (refreshed by a policy)
  * native compression after 7 days (typically 10-20x for meter data) and 1-year retention
"""

from __future__ import annotations

import logging
from pathlib import Path

from meterstream.models import AlertKind, Incident, Reading

SCHEMA = Path(__file__).with_name("schema.sql")
OPTIONAL_SCHEMA = Path(__file__).with_name("schema_tsl.sql")
log = logging.getLogger("meterstream")


class TimescaleStore:
    def __init__(self, dsn: str):
        import psycopg  # lazy: the memory store doesn't need it

        self._conn = psycopg.connect(dsn, autocommit=True)
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA.read_text())
        # Compression, retention and the 15-minute continuous aggregate need the Timescale License
        # edition. Managed Postgres services may ship only the Apache-2 edition; fall back to raw queries.
        self.has_rollup = True
        try:
            with self._conn.transaction(), self._conn.cursor() as cur:
                cur.execute(OPTIONAL_SCHEMA.read_text())
        except psycopg.Error as exc:
            self.has_rollup = False
            log.warning("TimescaleDB optional features unavailable (%s); using raw time_bucket queries", exc)

    def write_readings(self, readings: list[Reading]) -> int:
        if not readings:
            return 0
        # COPY into a temp table, then one INSERT ... ON CONFLICT: much faster than row-by-row inserts.
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE IF NOT EXISTS readings_in (LIKE readings INCLUDING DEFAULTS) ON COMMIT DROP")
            cols = "meter_id, ts, seq, energy_import_kwh, energy_export_kwh, power_kw, voltage_v"
            with cur.copy(f"COPY readings_in ({cols}) FROM STDIN") as copy:
                for r in readings:
                    copy.write_row(
                        (
                            r.meter_id,
                            _ts(r.ts),
                            r.seq,
                            r.energy_import_kwh,
                            r.energy_export_kwh,
                            r.power_kw,
                            r.voltage_v,
                        )
                    )
            cur.execute("INSERT INTO readings SELECT * FROM readings_in ON CONFLICT (meter_id, ts) DO NOTHING")
            return cur.rowcount

    def upsert_incidents(self, incidents: list[Incident]) -> None:
        if not incidents:
            return
        with self._conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO incidents (id, meter_id, kind, severity, start_ts, end_ts, alerts, detail, peak_value, open)
                VALUES (%s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET end_ts = EXCLUDED.end_ts, alerts = EXCLUDED.alerts,
                    detail = EXCLUDED.detail, peak_value = EXCLUDED.peak_value, open = EXCLUDED.open
                """,
                [
                    (
                        i.id,
                        i.meter_id,
                        i.kind.value,
                        i.severity.value,
                        i.start_ts,
                        i.end_ts,
                        i.alerts,
                        i.detail,
                        i.peak_value,
                        i.open,
                    )
                    for i in incidents
                ],
            )

    def readings(self, meter_id: str, start: int, end: int, bucket_s: int = 900) -> list[dict]:
        source = "readings_15m" if bucket_s == 900 and self.has_rollup else None
        with self._conn.cursor() as cur:
            if source:
                cur.execute(
                    """SELECT extract(epoch FROM bucket)::bigint, avg_power_kw, max_power_kw, min_voltage_v,
                              max_voltage_v, energy_kwh, samples
                       FROM readings_15m
                       WHERE meter_id = %s AND bucket >= to_timestamp(%s) AND bucket < to_timestamp(%s)
                       ORDER BY bucket""",
                    (meter_id, start, end),
                )
            else:
                cur.execute(
                    """SELECT extract(epoch FROM time_bucket(make_interval(secs => %s), ts))::bigint AS b,
                              avg(power_kw), max(power_kw), min(voltage_v), max(voltage_v),
                              max(energy_import_kwh) - min(energy_import_kwh), count(*)
                       FROM readings WHERE meter_id = %s AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s)
                       GROUP BY b ORDER BY b""",
                    (bucket_s, meter_id, start, end),
                )
            cols = ["bucket", "avg_power_kw", "max_power_kw", "min_voltage_v", "max_voltage_v", "energy_kwh", "samples"]
            return [
                dict(zip(cols, (float(v) if v is not None and i > 0 else v for i, v in enumerate(row)), strict=True))
                for row in cur.fetchall()
            ]

    def incidents(self, meter_id=None, kind=None, open_only=False, limit=100) -> list[dict]:
        where, args = [], []
        if meter_id:
            where.append("meter_id = %s")
            args.append(meter_id)
        if kind:
            where.append("kind = %s")
            args.append(kind)
        if open_only:
            where.append("open")
        sql = (
            "SELECT id, meter_id, kind, extract(epoch FROM start_ts)::bigint, extract(epoch FROM end_ts)::bigint, "
            "alerts, detail, peak_value, open FROM incidents"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY start_ts DESC LIMIT %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (*args, limit))
            out = []
            for id_, meter, k, s, e, n, detail, peak, is_open in cur.fetchall():
                inc = Incident(meter, AlertKind(k), s, e, n, detail, peak, is_open, id_)
                out.append(inc.to_dict())
            return out

    def fleet_summary(self) -> dict:
        with self._conn.cursor() as cur:
            cur.execute("SELECT count(DISTINCT meter_id), count(*), extract(epoch FROM max(ts))::bigint FROM readings")
            meters, readings, latest = cur.fetchone()
            cur.execute("SELECT kind, count(*) FROM incidents GROUP BY kind")
            by_kind = dict(cur.fetchall())
            cur.execute("SELECT count(*) FROM incidents WHERE open")
            (open_n,) = cur.fetchone()
        return {
            "meters": meters,
            "readings": readings,
            "incidents_by_kind": by_kind,
            "open_incidents": open_n,
            "latest_ts": latest,
        }


def _ts(epoch: int):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch, UTC)
