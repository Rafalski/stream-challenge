# Specification

Baseline agreed on 29 October 2026; Flink implementation authorised on 31 October 2026. Milestones 1 and 2 are reviewed; milestone 3 is implemented and locally verified. A focused milestone 4 increment, concurrent raw ingestion and Flink with end-to-end reporting, was authorised and verified locally on 1 November 2026, awaiting review. This file defines the required outcome. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records choices and open decisions.

## Scope

Build a straightforward, reliable and understandable streaming pipeline that runs locally using Docker. Assume fewer than 100,000 pageviews daily. No peak rate or latency target has been specified.

The pipeline generates sample events, ingests them in real time, archives original payloads and continuously counts pageviews by postcode and one-minute interval. Aggregates are stored separately. The repository provides source, local run instructions and design rationale.

The selected stack is Python, Kafka, PyFlink and local files, run through Docker Compose. The design uses event-time tumbling windows, independent raw ingestion and pytest scenarios. GitHub Actions remains planned.

## Event contract

Each pageview contains these four fields. Do not silently add or change fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `user_id` | integer | Uniquely identifies the user |
| `postcode` | string | User's current location |
| `webpage` | string | Visited URL |
| `timestamp` | integer (bigint) | Unix epoch seconds when the pageview occurred |

Example event:

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

Each aggregate must contain the postcode, the datetime identifying its one-minute aggregation window and the number of pageviews for that postcode and window. Write it under a separate local subdirectory/path from raw events. The implemented schema is `postcode`, UTC `window_start` and `pageview_count`; the window ends one minute after its start. No user-stream join or enrichment is requested.

## Base data-quality requirements

These checks form part of the base scope and do not depend on optional monitoring tools.

- Validate that each payload is valid JSON containing an object with the required fields and types. Reject missing/null required values and booleans used as integers. Do not silently coerce or repair values.
- Preserve original payloads in the raw archive, including malformed or rejected records. Exclude invalid records from aggregation and prevent their timestamps from advancing watermarks. Continue processing valid records when individual records fail validation.
- Make invalid and late-event outcomes visible through counts and structured reason codes. Include Kafka topic, partition and offset where available so a rejected record can be located without logging its payload. Keep validation failures distinct from storage, broker or processing failures; operational failures must not be silently skipped as bad data.
- Verify output quality independently in end-to-end scenarios: required aggregate fields, valid minute boundaries, positive integer counts and one final result per postcode/window under the agreed final-output policy. If corrections are selected, define how final results are identified.
- Reconcile known test inputs with validation outcomes and aggregate counts, accounting for late exclusions and replay under the agreed policies. Check counts by postcode/window as well as totals. Compare raw records and outputs only after the scenario's observable completion conditions are met, not by assuming both consumers progress together.

The agreed baseline rejects extra fields and blank postcode/webpage strings, preserves non-blank strings unchanged, and accepts integer identifiers without an added sign restriction. Timestamps must be epoch seconds representable as UTC datetimes (years 1-9999) and no more than five minutes ahead of processing time by default. Historical events remain valid. Late records for closed windows are excluded with diagnostics, without changing raw archival. Do not require full postcodes or URL schemes, add source fields or deduplicate identical pageviews without a separate decision.

## Requirements and verification

The table maps requirements to components and verification. Milestone verification is recorded in the roadmap and guide. Kafka smoke checks do not establish application correctness.

| ID | Requirement | Component | Verification |
| --- | --- | --- | --- |
| R1 | Generate sample events using the four-field contract above | Producer | Unit tests for types and sample values |
| R2 | Publish JSON events to the source broker | Producer and Kafka | Real publish/consume test, including send errors |
| R3 | Archive immutable original payloads without transformation | Independent raw ingestion and local files | Byte comparison with published payloads |
| R4 | Continuously aggregate the stream | PyFlink job | Exercise the real streaming job with Kafka input |
| R5 | Count pageviews by postcode and one-minute interval | Aggregation | Expected counts for multiple postcodes, windows and boundaries |
| R6 | Store postcode, window datetime and pageview count separately from raw events | Aggregate sink | Check content, datetime meaning and separate path |
| R7 | Apply base DQ validation and explicit invalid, out-of-order and late-event handling | Validation, raw archive and event-time policy | Unit and real-stream tests for rejection, preservation, continued processing and reason counts |
| R8 | Run and test locally using Docker; automate setup | Docker Compose and setup | Clean-environment start, demonstration and stop |
| R9 | Include automated tests using pytest | Test suites | Validation, timestamp and applicable window units; Kafka, Flink and file integration tests |
| R10 | Provide a scenario-driven end-to-end harness | Scenario tests | Named inputs and expected raw/aggregate/error results; isolation, observable completion, comparison, report and cleanup |
| R11 | Use structured logging, explicit errors and basic operational visibility | Producer, ingestion and job | Inspect logs, progress and failure behaviour |
| R12 | Automate checks with GitHub Actions | CI | Successful run of agreed checks on a clean runner |
| R13 | Provide run instructions; also document generation, inspection, tests, monitoring status and limitations | README, demo guides and optional results viewer | Follow verified instructions; inspect finished output and postcode/minute filters |
| R14 | Design for fewer than 100,000 pageviews daily | Design and local setup | Review resources and successful demonstration; no inferred peak-rate guarantee |
| R15 | Supply source through a publicly accessible GitHub repository and keep it public-safe | Repository; publication by engineer | Content review and engineer verification of public source availability |
| R16 | Explain design choices; record alternatives, AI workflow and separate production considerations | Engineering guide and agent rules | Engineer review against scope |
| R17 | Check aggregate quality and independently reconcile known input/output counts | End-to-end validation | Per-postcode/window and total comparisons, with invalids, late exclusions and replay accounted for |
| R18 | Provide default-enabled local run-duration, archive-budget and free-space safeguards, with explicit overrides/opt-out and no automatic data deletion | Producer and raw ingester | Focused boundary checks; real Kafka verification that a blocked write does not advance its offset |
| O1 | Optional monitoring and alerting, using Prometheus/Grafana if selected | Existing integrations and dashboards | Verify available throughput, invalid-event, output, lag, health, error, checkpoint/recovery and agreed alert signals |
| O2 | Optional Agent Forge incident investigation | Authorised isolated archive-limit failure and read-only evidence adapter; DQ defect remains later work | Real failure capture, immutable investigation/tool history, bounded AI calls, independent archive/offset checks and human review; no recovery actions |

R1-R18 define the project scope; implementation and review status are tracked in the roadmap. R18 is optional at execution time through an explicit opt-out, enabled by default. O1 and O2 do not block the core demonstration. A lightweight O1 increment is authorised: Flink metrics, Prometheus and a provisioned Grafana dashboard. Consumer lag through an existing Kafka exporter is also authorised. Raw-ingester exporters, alerts and recovery monitoring remain outside this increment. O2 now includes the authorised local archive-limit investigation only; its DQ defect and recovery extensions remain deferred. CI automation is planned and Mermaid diagrams document the design; further security hardening and alerting remain separate work. Confidentiality and secret exclusion always apply.

## Delivery constraints

Use Python, Kafka, PyFlink, Docker Compose and local filesystem storage. Raw ingestion and aggregation have independent responsibilities. Prefer direct aggregate output from Flink; add another Kafka topic only with a clear reason. AWS, Kinesis and Terraform are outside the chosen implementation.

Keep dependencies and code simple. Test the actual streaming behaviour rather than duplicating aggregation inside tests. Use observable conditions with bounded deadlines instead of arbitrary sleeps. Make recovery and delivery claims only where supported by evidence.

The per-record archive is for bounded local demonstrations. Default safety budgets are lower than a full day at the assumed upper volume; sustained daily-volume capability has not been verified. Before extended-volume or production use, agree and verify rolled byte-preserving storage, retention and recovery. Local simplicity must not hide a known unbounded-growth problem or imply that cloud deployment removes it.

Document in concise British English using Markdown and Mermaid. Keep source data unchanged and use generic terminology. Use synthetic examples and exclude identifying information, private source material and credentials.

The engineer reviews decisions and owns Git history. No agent commits, pushes, PR creation/merging or visibility changes are authorised. Implementation starts only after specification review and authorisation of the next milestone.

## Acceptance

Demonstrate generation, Kafka publication, unchanged raw archival, correct postcode/window/count records, separate aggregate files and documented error behaviour. Run the relevant tests and verify the README walkthrough. The engineer handles public publication; its requirement does not authorise agent pushes or repository visibility changes.

The verified Flink baseline uses DataStream processing, JSONL output with `postcode`, UTC `window_start` and `pageview_count`, five-second disorder tolerance, final-only windows and ten-second checkpoints. A quiet stream can leave its last window open. Versions and runtime evidence are recorded in the engineering guide; broader recovery guarantees and production storage remain deferred.
