-- Optional features from the Timescale License edition (self-hosted / Timescale Cloud).
-- Applied after schema.sql; skipped automatically if the server doesn't support them.

-- Compress chunks older than 7 days, segmented by meter so per-meter queries stay fast.
ALTER TABLE readings SET (timescaledb.compress, timescaledb.compress_segmentby = 'meter_id',
                          timescaledb.compress_orderby = 'ts');
SELECT add_compression_policy('readings', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('readings', INTERVAL '365 days', if_not_exists => TRUE);

-- 15-minute rollup that dashboards read instead of raw rows.
CREATE MATERIALIZED VIEW IF NOT EXISTS readings_15m
WITH (timescaledb.continuous) AS
SELECT meter_id,
       time_bucket(INTERVAL '15 minutes', ts)             AS bucket,
       avg(power_kw)                                      AS avg_power_kw,
       max(power_kw)                                      AS max_power_kw,
       min(voltage_v)                                     AS min_voltage_v,
       max(voltage_v)                                     AS max_voltage_v,
       max(energy_import_kwh) - min(energy_import_kwh)    AS energy_kwh,
       count(*)                                           AS samples
FROM readings
GROUP BY meter_id, bucket
WITH NO DATA;
SELECT add_continuous_aggregate_policy('readings_15m', start_offset => INTERVAL '3 days',
       end_offset => INTERVAL '15 minutes', schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
-- Real-time aggregation: include not-yet-materialized recent data in queries.
ALTER MATERIALIZED VIEW readings_15m SET (timescaledb.materialized_only = false);
