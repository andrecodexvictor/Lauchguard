-- Two real Confluent ML_FORECAST calls, 10-window warm-up, model carried across releases.
CREATE MATERIALIZED TABLE impact_forecast (
  service STRING, version STRING, ts TIMESTAMP(3),
  failure_rate DOUBLE, failed_gmv DOUBLE,
  forecast_failure_rate DOUBLE, forecast_failed_gmv DOUBLE,
  projected_gmv_at_risk_hour DOUBLE
) WITH ('value.format' = 'avro-registry') AS
WITH predictions AS (
  SELECT service, version, ts, failure_rate, failed_gmv,
    ML_FORECAST(failure_rate, ts,
      JSON_OBJECT('p' VALUE 1, 'd' VALUE 0, 'q' VALUE 1,
                  'minTrainingSize' VALUE 10, 'maxTrainingSize' VALUE 60,
                  'enableStl' VALUE false, 'horizon' VALUE 1))
      OVER (PARTITION BY service ORDER BY ts
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS failure_prediction,
    ML_FORECAST(failed_gmv, ts,
      JSON_OBJECT('p' VALUE 1, 'd' VALUE 0, 'q' VALUE 1,
                  'minTrainingSize' VALUE 10, 'maxTrainingSize' VALUE 60,
                  'enableStl' VALUE false, 'horizon' VALUE 1))
      OVER (PARTITION BY service ORDER BY ts
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gmv_prediction
  FROM release_metrics
)
SELECT service, version, ts, failure_rate, failed_gmv,
       LEAST(1.0, GREATEST(0.0, failure_prediction.forecast_value)) AS forecast_failure_rate,
       GREATEST(0.0, gmv_prediction.forecast_value) AS forecast_failed_gmv,
       GREATEST(0.0, gmv_prediction.forecast_value) * 360.0 AS projected_gmv_at_risk_hour
FROM predictions;
