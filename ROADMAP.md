# Roadmap

Milestone 0 is a documentation draft awaiting review. All implementation is unstarted. Checkboxes mean verified completion and engineer review.

Work in order, with approval before each next milestone. Tests and documentation accompany features from the start; milestones 4 and 5 consolidate coverage and harden behaviour. Optional milestones 6 and 8 may be explicitly deferred.

| Done | Milestone and scope | Deliverables and acceptance | Tests/checks | Dependencies |
| --- | --- | --- | --- | --- |
| [ ] | 0. Specification and architecture | Agree requirements, architecture and decisions needed for the next step | Documentation links, sample values, traceability and consistency review | Current brief; engineer approval pending |
| [ ] | 1. Docker infrastructure and Kafka | Reproducible setup, readiness checks, topic configuration and verified start/stop instructions | Broker connectivity, topic access and restart smoke checks | 0; compatible versions and local topology agreed |
| [ ] | 2. Producer and raw ingestion | Sample events reach Kafka; independent ingestion preserves original payloads locally | Event contract, publish/consume, byte fidelity and write/send failure tests | 1; raw format, offsets and validation policy agreed |
| [ ] | 3. PyFlink aggregation | Continuous postcode counts reach a separate directory; base DQ rejects invalid input with visible reasons while valid input continues | Real Kafka/Flink tests for boundaries, ordering, invalid/late input, raw preservation and output visibility | 2; API, sink, DQ, event-time and checkpoint policies agreed |
| [ ] | 4. Unit and end-to-end testing | Repeatable harness isolates resources, checks aggregate quality, reconciles known input/output counts, reports and cleans up | Happy path, multiple windows/postcodes, missing/invalid values, late/out-of-order input and quiet streams; account for exclusions and replay | 3; completion protocol agreed |
| [ ] | 5. Reliability and error handling | Recovery, retries and failures behave as documented; core demonstration passes | Interruptions, restart/replay, failed writes, consumer independence and checkpoints if enabled | 4; delivery guarantees agreed |
| [ ] | 6. Prometheus and Grafana: optional | Existing integrations expose useful signals; agree alerts if included and document unavailable metrics | Scraping, known traffic, failure visibility and any agreed alerts | 5; explicit agreement to include |
| [ ] | 7. CI automation | GitHub Actions runs agreed checks and provides useful failure evidence | Clean-runner unit/integration execution; document any exclusions | 5; 6 completed or deferred |
| [ ] | 8. Agent Forge investigation: deferred, optional | Isolated controlled defect, independent detection, investigation and proposed regression test; approval before a fix branch or PR | Healthy baseline, defect reproduction and independent validation | 7; monitoring if needed; separate authorisation |
| [ ] | 9. Documentation and final review | Verified README walkthrough, requirement coverage and public-safe source; engineer handles public GitHub availability | Full demonstration, required suites, content review and engineer publication check | 7; optional work completed or deferred |

Mandatory functionality is covered by milestones 1-5. CI and final review remain required engineering work. No milestone is complete yet.

Security, automation and diagrams are bonuses in the brief. Our plan already includes CI and Mermaid diagrams; additional hardening and alerting remain optional and need an agreed scope.

At each gate, report changes, verification results, outstanding issues and a short suggested commit message. The engineer reviews and commits locally.
