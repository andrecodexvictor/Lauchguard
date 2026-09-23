-- Kafka record timestamps are the event-time source in the existing Avro contract.
CREATE MATERIALIZED TABLE checkout_enriched (
  event_id STRING, service STRING, version STRING,
  region STRING, payment_method STRING, success BOOLEAN,
  amount DOUBLE, latency_ms INT, event_ts TIMESTAMP_LTZ(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH ('value.format' = 'avro-registry') AS
SELECT c.event_id, c.service, d.version, c.region, c.payment_method,
       c.success, c.amount, c.latency_ms, c.`$rowtime` AS event_ts
FROM checkout_events AS c
JOIN deployment_state FOR SYSTEM_TIME AS OF c.`$rowtime` AS d
  ON c.service = d.service;
