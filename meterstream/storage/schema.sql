-- Core schema, applied on startup by TimescaleStore (idempotent). Works on TimescaleDB Apache-2 edition.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS readings (
    meter_id           text             NOT NULL,
    ts                 timestamptz      NOT NULL,
    seq                bigint           NOT NULL,
    energy_import_kwh  double precision NOT NULL,
    energy_export_kwh  double precision NOT NULL,
    power_kw           real             NOT NULL,
    voltage_v          real             NOT NULL,
    PRIMARY KEY (meter_id, ts)          -- dedupes at-least-once redeliveries
);
SELECT create_hypertable('readings', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS incidents (
    id          text PRIMARY KEY,
    meter_id    text        NOT NULL,
    kind        text        NOT NULL,
    severity    text        NOT NULL,
    start_ts    timestamptz NOT NULL,
    end_ts      timestamptz NOT NULL,
    alerts      int         NOT NULL,
    detail      text,
    peak_value  double precision,
    open        boolean     NOT NULL
);
CREATE INDEX IF NOT EXISTS incidents_meter_idx ON incidents (meter_id, start_ts DESC);
CREATE INDEX IF NOT EXISTS incidents_open_idx ON incidents (start_ts DESC) WHERE open;
