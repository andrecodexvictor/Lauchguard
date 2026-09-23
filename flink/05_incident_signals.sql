-- 35% is an illustrative abandonment factor; failed GMV is not permanently lost revenue.
CREATE MATERIALIZED TABLE incident_signals (
  service STRING, version STRING, ts TIMESTAMP(3),
  failure_rate DOUBLE, forecast_failure_rate DOUBLE,
  projected_gmv_at_risk_hour DOUBLE, expected_revenue_loss_hour DOUBLE,
  severity STRING
) WITH ('value.format' = 'avro-registry') AS
SELECT service, version, ts, failure_rate, forecast_failure_rate,
       projected_gmv_at_risk_hour,
       projected_gmv_at_risk_hour * 0.35 AS expected_revenue_loss_hour,
       CASE WHEN forecast_failure_rate >= 0.12 THEN 'CRITICAL'
            WHEN forecast_failure_rate >= 0.07 THEN 'HIGH'
            WHEN forecast_failure_rate >= 0.04 THEN 'WARNING'
            ELSE 'HEALTHY' END AS severity
FROM impact_forecast;
