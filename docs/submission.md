# Developer Day submission packet

Form: https://docs.google.com/forms/d/e/1FAIpQLSeNcjEcA3wYG6hzIIdtRG1IbeA4owiMMNCAA5lpNDducI4GJA/viewform

The form describes the Most Impactful App prize and considers business impact, Confluent connectors, stream processing and stream governance. It explicitly marks the screenshot field **NO AI Usage Allowed**. Capture the actual app or Stream Lineage; do not generate, retouch, composite or fabricate evidence. The form has not been submitted.

## Fields

| Form field | Prepared answer / remaining input |
| --- | --- |
| Current location | User to provide city and country |
| First name / last name | User to confirm submission name |
| Confluent Cloud email | User to enter directly in the form |
| Job title / company | User to provide; do not invent |
| GitHub repository | https://github.com/andrecodexvictor/Lauchguard |
| Application description | Draft below; update after end-to-end validation |
| Screenshot URL, required | Pending genuine live evidence and user-approved public hosting |
| Connectors used | No connector verified yet; do not claim HTTP Sink until configured and receiving real signals |
| Schema | Exact bootstrap Avro contracts: [checkout](checkout_events.avsc), [deployment](deployment_events.avsc) |

## Application description draft

LaunchGuard is Predictive Release Intelligence for e-commerce SRE and financial operations teams. Its goal is to reduce the time between a harmful checkout release and an operator-approved rollback, while making the business exposure visible. Kafka carries checkout and deployment events under Schema Registry Avro contracts. The proposed Confluent Flink pipeline correlates each checkout with the release active at event time, aggregates 10-second windows, applies ML_FORECAST to failure rate and failed GMV, and produces severity and hourly exposure signals. FastAPI displays these outputs and publishes real deployment or rollback events. Expected revenue loss applies a documented illustrative 35% abandonment factor; failed GMV is not treated as permanently lost revenue. Revenue Protected measures the observed reduction in expected hourly exposure after sustained recovery. LaunchGuard does not use an LLM to decide that an incident exists.

This draft describes the intended full pipeline. Do not change “proposed” to a completed integration until the live Flink outputs and rollback sequence are verified.

## Evidence checklist

- [x] Python simulator, control endpoints and plain HTML dashboard implemented.
- [x] Offline tests: distributions, API actions, failed delivery, stale pipeline and sustained recovery.
- [x] Existing cloud environment and Kafka cluster verified in the authenticated browser.
- [x] Existing launchguard-flink pool verified, AWS us-east-2, max 10 CFUs, current use 0.
- [x] Both input Avro subjects confirmed at version 1 in Schema Registry.
- [ ] Continuous Kafka traffic acknowledged from the application.
- [ ] Temporal correlation verified across deployment and rollback boundaries.
- [ ] release_metrics, impact_forecast and incident_signals producing real rows.
- [ ] Forecast warm-up, incident escalation and recovery shown live.
- [ ] HTTP Sink connector delivers incident_signals to a temporary webhook.
- [ ] Real screenshot of populated Stream Lineage or app captured by the participant.
- [ ] Screenshot hosted at a URL accessible to judges.
- [ ] Identity fields reviewed and form submitted by the participant.

## Current blockers

Schema Registry authentication is now verified with the resource-scoped key, and both input subjects are present. Earlier HTTP 401 responses were caused by attempting to use a Global key with public Schema Registry; this combination is unsupported by Confluent. Credentials remain only in the ignored local `.env`.

The remote execution environment cannot resolve the Kafka bootstrap hostname. Kafka credentials and live traffic therefore remain unverified. Run the Python application from a machine/network with DNS access to the configured broker and outbound TCP 9092. `python -m scripts.preflight` checks Registry authentication, broker DNS, Kafka metadata and the two input topics independently, without displaying credentials.

The actual FastAPI lifespan was tested with the available credentials: the dashboard and status API return HTTP 200 and accurately display the waiting state when Kafka is unreachable. This is not an end-to-end streaming test.

All five SQL candidate cells are saved in the existing SQL Workspace. The catalog recognizes both input tables with their expected Avro fields and Kafka `$rowtime`. No pipeline statement has been started. The console quoted $0.0035 per CFU per minute with a 10-CFU pool maximum (up to $2.10/hour for Flink compute alone); the account owner must authorize a spending limit before activation. Kafka and connector charges are separate.
