# Developer Day submission packet

Form: https://docs.google.com/forms/d/e/1FAIpQLSeNcjEcA3wYG6hzIIdtRG1IbeA4owiMMNCAA5lpNDducI4GJA/viewform

The form describes the Most Impactful App prize and considers business impact, Confluent connectors, stream processing and stream governance. It explicitly marks the screenshot field **NO AI Usage Allowed**. Capture the actual app or Stream Lineage; do not generate, retouch, composite or fabricate evidence. The form was submitted on September 24, 2026. Google Forms confirmed: “Thank you! Your App has been submitted for AI Day.”

## Fields

| Form field | Prepared answer / remaining input |
| --- | --- |
| Current location | User to provide city and country |
| First name / last name | User to confirm submission name |
| Confluent Cloud email | User to enter directly in the form |
| Job title / company | User to provide; do not invent |
| GitHub repository | https://github.com/andrecodexvictor/Lauchguard |
| Application description | Draft below; update after end-to-end validation |
| Screenshot URL, required | https://raw.githubusercontent.com/andrecodexvictor/Lauchguard/codex/launchguard-reliability-submission/docs/stream-lineage-full-20260924.png — genuine browser capture of both input branches |
| Connectors used | No connector verified yet; do not claim HTTP Sink until configured and receiving real signals |
| Schema | Exact bootstrap Avro contracts: [checkout](checkout_events.avsc), [deployment](deployment_events.avsc) |

The personal fields above require the participant's own current location, name split, Confluent Cloud email, job title and company. Do not submit invented values. For the connector question, enter **None currently**; an HTTP Sink is a planned integration, not an active connector.

Paste both schemas into the schema field if it accepts multiple records:

```json
{"type":"record","name":"CheckoutEvent","namespace":"launchguard","fields":[{"name":"event_id","type":"string"},{"name":"customer_id","type":"string"},{"name":"service","type":"string"},{"name":"region","type":"string"},{"name":"plan","type":"string"},{"name":"payment_method","type":"string"},{"name":"success","type":"boolean"},{"name":"amount","type":"double"},{"name":"latency_ms","type":"int"}]}
{"type":"record","name":"DeploymentEvent","namespace":"launchguard","fields":[{"name":"deployment_id","type":"string"},{"name":"service","type":"string"},{"name":"version","type":"string"},{"name":"action","type":"string"}]}
```

## Application description draft

LaunchGuard is Predictive Release Intelligence for e-commerce SRE and financial operations teams. It aims to shorten the time between a harmful checkout release and an operator-approved rollback while exposing business impact. Kafka carries checkout and deployment events governed by Avro schemas in Schema Registry. Three Confluent Flink materialized tables have been created: deployment_state maintains the active release, checkout_enriched correlates events with that release using a temporal join, and release_metrics aggregates 10-second windows of failure rate, latency and failed GMV. A Python/FastAPI simulator and dashboard are implemented, and demonstration events have been written into both input topics using Flink SQL. The planned ML_FORECAST and incident stages would calculate forecast failure rate, GMV at risk, and expected hourly revenue loss using an illustrative 35% abandonment factor; failed GMV is not assumed to be permanently lost. These latter stages are not yet running. LaunchGuard does not use an LLM to decide that an incident exists.

Do not claim a completed forecast, incident signal, HTTP Sink, or live rollback sequence until verified. The linked screenshot shows real lineage from both input topics through the first Flink stages; it does not prove the whole pipeline.

## Evidence checklist

- [x] Python simulator, control endpoints and plain HTML dashboard implemented.
- [x] Offline tests: distributions, API actions, failed delivery, stale pipeline and sustained recovery.
- [x] Existing cloud environment and Kafka cluster verified in the authenticated browser.
- [x] Existing launchguard-flink pool verified, AWS us-east-2, max 10 CFUs, current use 0.
- [x] Both input Avro subjects confirmed at version 1 in Schema Registry.
- [x] deployment_state, checkout_enriched and release_metrics created in the existing Flink SQL Workspace.
- [x] One stable deployment and 20 synthetic checkouts written by successful Flink SQL INSERT statements.
- [x] Real Stream Lineage graph captured: 4 topics, 9 applications, 21 messages in and 21 messages out at capture time.
- [ ] Continuous Kafka traffic acknowledged from the application.
- [ ] Temporal correlation verified across deployment and rollback boundaries.
- [ ] release_metrics, impact_forecast and incident_signals producing real rows.
- [ ] Forecast warm-up, incident escalation and recovery shown live.
- [ ] HTTP Sink connector delivers incident_signals to a temporary webhook.
- [x] Real screenshot of populated Stream Lineage captured in the authenticated Confluent browser.
- [x] Screenshot hosted in the public GitHub branch for judge access.
- [x] Required identity and affiliation fields completed, and the form submitted; confirmation page observed.

## Current blockers

Schema Registry authentication is now verified with the resource-scoped key, and both input subjects are present. Earlier HTTP 401 responses were caused by attempting to use a Global key with public Schema Registry; this combination is unsupported by Confluent. Credentials remain only in the ignored local `.env`.

The remote execution environment cannot resolve the Kafka bootstrap hostname. Kafka credentials and live traffic therefore remain unverified. Run the Python application from a machine/network with DNS access to the configured broker and outbound TCP 9092. `python -m scripts.preflight` checks Registry authentication, broker DNS, Kafka metadata and the two input topics independently, without displaying credentials.

The actual FastAPI lifespan was tested with the available credentials: the dashboard and status API return HTTP 200 and accurately display the waiting state when Kafka is unreachable. This is not an end-to-end streaming test.

The first three SQL cells have created materialized tables. A stable deployment INSERT completed and a 20-row checkout INSERT completed in the workspace. Stream Lineage now displays both input branches converging at `checkout_enriched` and continuing toward `release_metrics`. The fourth SQL cell fails validation while extracting nested `ML_FORECAST` results; `impact_forecast` and `incident_signals` have not been created. No HTTP Sink exists. The compute pool is active and may incur ongoing usage; account owner should review billing after the submission. Kafka and connector charges are separate.
