# Roadmap

Milestones 0-2 are reviewed. Milestone 3 is implemented and verified locally. On 1 November 2026, the engineer authorised the next increment: concurrent raw ingestion and Flink in the end-to-end demonstration, with clear reporting. This milestone 4 increment is implemented and verified locally, awaiting review. The engineer has also authorised a focused optional monitoring increment; recovery and CI remain unstarted. Checkboxes mean verified completion and engineer review.

The same demo scope now includes a three-phase live stream, uneven postcode counts and an optional read-only Kafka browser. A 1,224-record live run, committed output, raw archival and browser inspection were verified locally. This is separate from the 13-record assertion-based end-to-end scenario; Prometheus/Grafana monitoring is a separate authorised increment below.

The current documentation increment adds separate Bash/PowerShell demo guides, timing explanations and Arrange/Act/Assert comments in the existing scenario; executable test behaviour is unchanged. A Git Bash checkpoint-path conversion failure was diagnosed and corrected in the shell instructions; Windows tests and direct Git Bash execution are verified separately from unverified native WSL execution.

Work in order, with approval before each next milestone. Tests and documentation accompany features from the start; milestones 4 and 5 consolidate coverage and harden behaviour. Optional milestones 6 and 8 may be explicitly deferred.

| Done | Milestone and scope | Deliverables and acceptance | Tests/checks | Dependencies |
| --- | --- | --- | --- | --- |
| [x] | 0. Specification and architecture | Requirements and local implementation route agreed; later design decisions remain open | Documentation links, sample values, traceability and consistency reviewed | Engineer authorised milestone 1 on 29 October 2026 |
| [x] | 1. Docker infrastructure and Kafka | Compose setup, readiness checks, topic initialization and run instructions reviewed locally by engineer | Startup, repeat initialization, real publish/consume, persistence, shutdown and reset verified | 0; engineer authorised milestone 2 |
| [x] | 2. Producer and raw ingestion | Producer, raw ingester, per-record archive and default-on optional local safeguards reviewed | Core units and four real-Kafka checks pass; Docker output and demo controls verified; blocked writes leave offsets uncommitted | 1; engineer authorised milestone 3 |
| [ ] | 3. PyFlink aggregation | DataStream job, DQ, JSONL sink and checkpoints implemented; awaiting review | 59 local units, four Kafka checks and one continuous Kafka/Flink scenario pass: boundaries, ordering, invalid/late input, raw preservation, committed output and quiet windows | 2; baseline and compatible runtime verified |
| [ ] | 4. Unit and end-to-end testing | Current increment: concurrent raw/Flink containers, isolated inputs, exact raw/aggregate comparisons and concise reporting; verified, awaiting review | 59 units, four Kafka checks and the concurrent end-to-end scenario pass; readiness, raw byte fidelity, four aggregates, DQ reconciliation, quiet windows and cleanup verified. Broader scenario coverage and replay remain later work | 3; engineer authorised this focused increment |
| [ ] | 5. Reliability and error handling | Recovery, retries and failures behave as documented; core demonstration passes | Interruptions, restart/replay, failed writes, consumer independence and checkpoints if enabled | 4; delivery guarantees agreed |
| [ ] | 6. Prometheus and Grafana: optional | Authorised ahead of recovery work: bundled Flink reporter, Kafka consumer-lag exporter, optional services and provisioned live dashboard; implemented and verified, awaiting review | Three healthy scrape targets, all 15 panel queries, 742 paced records, DQ exporter counts, committed files and advancing checkpoints verified; consumer lag verified against Kafka offsets and paced traffic; job-name default with optional submission filter; monitoring stops independently; no alerts | Focused increment authorised by engineer; milestone 5 remains deferred |
| [ ] | 7. CI automation | GitHub Actions runs agreed checks and provides useful failure evidence | Clean-runner unit/integration execution; document any exclusions | 5; 6 completed or deferred |
| [ ] | 8. Agent Forge investigation: deferred, optional | Isolated controlled defect, independent detection, investigation and proposed regression test; approval before a fix branch or PR | Healthy baseline, defect reproduction and independent validation | 7; monitoring if needed; separate authorisation |
| [ ] | 9. Documentation and final review | Verified README walkthrough, requirement coverage and public-safe source; engineer handles public GitHub availability | Full demonstration, required suites, content review and engineer publication check | 7; optional work completed or deferred |

Mandatory functionality is covered by milestones 1-5. CI and final review remain required engineering work. The engineer authorised the focused end-to-end increment above; monitoring is separately authorised, without completing broader testing or recovery. This does not mark all milestone 4 coverage or milestone 5 recovery checks complete.

Before extended-volume or production use, separately agree byte-preserving rolled archives, retention and recovery evidence. This is a readiness gate, not a claim that the current bounded local archive supports sustained daily volume.

Security, automation and diagrams are bonuses in the brief. Our plan already includes CI and Mermaid diagrams; additional hardening and alerting remain optional and need an agreed scope.

At each gate, report changes, verification results, outstanding issues and a short suggested commit message. The engineer reviews and commits locally.
