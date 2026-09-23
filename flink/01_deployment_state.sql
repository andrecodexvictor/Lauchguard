-- Run in launchguard-devday / launchguard-kafka, streaming mode.
CREATE MATERIALIZED TABLE deployment_state (
  service STRING, version STRING, action STRING,
  PRIMARY KEY (service) NOT ENFORCED
) WITH ('value.format' = 'avro-registry') AS
SELECT service, LAST_VALUE(version) AS version, LAST_VALUE(action) AS action
FROM deployment_events GROUP BY service;
