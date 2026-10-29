# Specification

Baseline agreed and milestone 1 authorised on 29 October 2026. This file defines the required outcome. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records choices and open decisions. Local Kafka infrastructure is implemented; the streaming application is not yet implemented.

## Scope

Build a straightforward, reliable and understandable streaming pipeline that runs locally using Docker. Assume fewer than 100,000 pageviews daily. No peak rate or latency target has been specified.

The exercise requires sample event production, real-time ingestion, unchanged archival, continuous aggregation by postcode and one-minute interval, separate aggregate storage, a publicly accessible GitHub repository, run instructions and explainable design choices.

The brief permits Kafka or Kinesis input, local files or S3 storage, and a choice of streaming technology. We have selected Python, Kafka, PyFlink and local files, run through Docker Compose. Event-time tumbling windows, independent raw ingestion, pytest, scenario tests and GitHub Actions are project decisions or engineering additions, rather than technologies mandated by the exercise. AWS-specific guidance applies only to the unselected alternative route.

## Event contract

Each pageview contains these four fields. Do not silently add or change fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `user_id` | integer | Uniquely identifies the user |
| `postcode` | string | User's current location |
| `webpage` | string | Visited URL |
| `timestamp` | integer (bigint) | Unix epoch seconds when the pageview occurred |

Sample supplied with the brief:

```json
{
  "user_id": 1234,
  "postcode": "SW19",
  "webpage": "www.website.com/index.html",
  "timestamp": 1611662684
}
```

These are example values, not constants for every generated event. The sample postcode is a partial postcode and the webpage has no URL scheme. Do not require a full UK postcode or an `https://` prefix without an agreed change. Pageview events are immutable; archive their original payloads without transformations.

## Aggregate content

Each aggregate must contain the postcode, the datetime identifying its one-minute aggregation window and the number of pageviews for that postcode and window. Write it under a separate local subdirectory/path from raw events. Field names, datetime representation and whether to include both window boundaries remain implementation decisions. No user-stream join or enrichment is requested.

## Base data-quality requirements

These are project additions, not extra requirements attributed to the brief. They form part of the proposed base scope and do not depend on optional monitoring tools.

- Validate that each payload is valid JSON containing an object with the required fields and types. Reject missing/null required values and booleans used as integers. Do not silently coerce or repair values.
- Preserve original payloads in the raw archive, including malformed or rejected records. Exclude invalid records from aggregation and prevent their timestamps from advancing watermarks. Continue processing valid records when individual records fail validation.
- Make invalid and late-event outcomes visible through counts and structured reason codes. Include Kafka topic, partition and offset where available so a rejected record can be located without logging its payload. Keep validation failures distinct from storage, broker or processing failures; operational failures must not be silently skipped as bad data.
- Verify output quality independently in end-to-end scenarios: required aggregate fields, valid minute boundaries, positive integer counts and one final result per postcode/window under the agreed final-output policy. If corrections are selected, define how final results are identified.
- Reconcile known test inputs with validation outcomes and aggregate counts, accounting for late exclusions and replay under the agreed policies. Check counts by postcode/window as well as totals. Compare raw records and outputs only after the scenario's observable completion conditions are met, not by assuming both consumers progress together.

Extra-field handling, blank strings, identifier ranges, timestamp bounds and late-event treatment still need agreement. Do not reject historical events merely because they are old: the supplied sample must remain valid. Do not require full postcodes or URL schemes, add source fields or deduplicate identical pageviews without a separate decision.

## Requirements and verification

The table is the requirements traceability matrix. Origin distinguishes exercise requirements from our project additions; components reflect our selected implementation. Milestone verification is recorded in the roadmap and guide. Kafka smoke checks do not establish application correctness.

| ID | Requirement | Origin | Component | Verification |
| --- | --- | --- | --- | --- |
| R1 | Generate sample events using the four-field contract above | Brief | Producer | Unit tests for types and sample values |
| R2 | Publish JSON events to the source broker | Brief | Producer and Kafka | Real publish/consume test, including send errors |
| R3 | Archive immutable original payloads without transformation | Brief | Independent raw ingestion and local files | Byte comparison with published payloads |
| R4 | Continuously aggregate the stream | Brief | PyFlink job | Exercise the real streaming job with Kafka input |
| R5 | Count pageviews by postcode and one-minute interval | Brief; event-time tumbling is our choice | Aggregation | Expected counts for multiple postcodes, windows and boundaries |
| R6 | Store postcode, window datetime and pageview count separately from raw events | Brief | Aggregate sink | Check content, datetime meaning and separate path |
| R7 | Apply base DQ validation and explicit invalid, out-of-order and late-event handling | Project | Validation, raw archive and event-time policy | Unit and real-stream tests for rejection, preservation, continued processing and reason counts |
| R8 | Run and test locally using Docker; automate setup | Brief for Docker on Kafka route; project for automation | Docker Compose and setup | Clean-environment start, demonstration and stop |
| R9 | Include automated tests using pytest | Project | Test suites | Validation, timestamp and applicable window units; Kafka, Flink and file integration tests |
| R10 | Provide a scenario-driven end-to-end harness | Project | Scenario tests | Named inputs and expected raw/aggregate/error results; isolation, observable completion, comparison, report and cleanup |
| R11 | Use structured logging, explicit errors and basic operational visibility | Project | Producer, ingestion and job | Inspect logs, progress and failure behaviour |
| R12 | Automate checks with GitHub Actions | Project | CI | Successful run of agreed checks on a clean runner |
| R13 | Provide run instructions; also document generation, inspection, tests, monitoring status and limitations | Brief for run instructions; project for detail | README and engineering guide | Follow verified instructions from a clean environment |
| R14 | Design for fewer than 100,000 pageviews daily | Brief | Design and local setup | Review resources and successful demonstration; no inferred peak-rate guarantee |
| R15 | Supply source through a publicly accessible GitHub repository and keep it public-safe | Brief for public access; project for confidentiality rules | Repository; publication by engineer | Content review and engineer verification of public source availability |
| R16 | Explain design choices; record alternatives, AI workflow and separate production considerations | Brief for rationale; project for supporting guides | Engineering guide and agent rules | Engineer review against scope |
| R17 | Check aggregate quality and independently reconcile known input/output counts | Project | End-to-end validation | Per-postcode/window and total comparisons, with invalids, late exclusions and replay accounted for |
| O1 | Optional monitoring and alerting, using Prometheus/Grafana if selected | Brief bonus; tools chosen by project | Existing integrations and dashboards | Verify available throughput, invalid-event, output, lag, health, error, checkpoint/recovery and agreed alert signals |
| O2 | Deferred optional Agent Forge incident investigation | Project | Isolated fault-injection demonstration | Healthy baseline, independent detection, evidence, proposed fix/regression test and human approval gate |

R1-R17 are required for our proposed project scope, with their origins shown above. O1 and O2 do not block the core demonstration. The brief also lists security, automation and diagrams as optional enhancements. We have chosen CI automation and Mermaid diagrams as project deliverables; further security hardening and alerting remain optional. Confidentiality and secret exclusion always apply.

## Delivery constraints

Use Python, Kafka, PyFlink, Docker Compose and local filesystem storage. Raw ingestion and aggregation have independent responsibilities. Prefer direct aggregate output from Flink; add another Kafka topic only with a clear reason. AWS, Kinesis and Terraform are outside the chosen implementation.

Keep dependencies and code simple. Test the actual streaming behaviour rather than duplicating aggregation inside tests. Use observable conditions with bounded deadlines instead of arbitrary sleeps. Make recovery and delivery claims only where supported by evidence.

Document in concise British English using Markdown and Mermaid. Keep source data unchanged and use generic terminology. Exclude company identities, recruitment context, original assessment material, proprietary information, personal data and credentials.

The engineer reviews decisions and owns Git history. No agent commits, pushes, PR creation/merging or visibility changes are authorised. Implementation starts only after specification review and authorisation of the next milestone.

## Acceptance

Demonstrate generation, Kafka publication, unchanged raw archival, correct postcode/window/count records, separate aggregate files and documented error behaviour. Run the relevant tests and verify the README walkthrough. The engineer handles public publication; its requirement does not authorise agent pushes or repository visibility changes.

API choice, versions, storage formats, aggregate schema, validation limits, watermark/late-event policy, checkpointing and delivery guarantees remain open in the engineering guide. Those choices must not be treated as additional requirements from the brief.
