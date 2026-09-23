CREATE MATERIALIZED TABLE release_metrics (
  service STRING, version STRING, ts TIMESTAMP(3),
  attempts BIGINT, failures BIGINT, failure_rate DOUBLE,
  avg_latency_ms DOUBLE, failed_gmv DOUBLE,
  WATERMARK FOR ts AS ts
) WITH ('value.format' = 'avro-registry') AS
SELECT service, version, CAST(window_end AS TIMESTAMP(3)) AS ts,
       COUNT(*) AS attempts,
       SUM(CASE WHEN success THEN 0 ELSE 1 END) AS failures,
       CAST(SUM(CASE WHEN success THEN 0 ELSE 1 END) AS DOUBLE) / COUNT(*) AS failure_rate,
       AVG(CAST(latency_ms AS DOUBLE)) AS avg_latency_ms,
       SUM(CASE WHEN success THEN 0.0 ELSE amount END) AS failed_gmv
FROM TABLE(TUMBLE(TABLE checkout_enriched, DESCRIPTOR(event_ts), INTERVAL '10' SECOND))
GROUP BY service, version, window_start, window_end;
