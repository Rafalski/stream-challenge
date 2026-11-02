# Roadmap

Documentation review on 2 November 2026: shortened the README, moved detailed commands into a local reference, repaired navigation text and simplified project wording. All 73 unit cases, four Kafka integration checks and the isolated Kafka/Flink scenario passed again. Local documentation links and Compose configuration also passed checks. Runtime behaviour is unchanged. CI, recovery work and final review are still outstanding.

Milestones 0-2 are complete and reviewed. The Flink implementation is working and has passed local checks. On 1 November 2026, work expanded to run raw ingestion and Flink together in the end-to-end demo, with clearer reporting. That part is implemented and locally verified, with review still pending. Optional monitoring is also implemented and checked; recovery and CI are still to do. Checked boxes mean both verification and review are complete.

The same demo scope now includes a three-phase live stream, uneven postcode counts and an optional read-only Kafka browser. A 1,224-record live run, committed output, raw archival and browser inspection were verified locally. This is separate from the 13-record assertion-based end-to-end scenario; Prometheus/Grafana monitoring is tracked separately below.

The demo documentation includes separate Bash/PowerShell demo guides, timing explanations and Arrange/Act/Assert comments in the existing scenario; executable test behaviour is unchanged. A Git Bash checkpoint-path conversion failure was diagnosed and corrected in the shell instructions; Windows tests and direct Git Bash execution are verified separately from unverified native WSL execution.

The results viewer adds a read-only Python table, postcode/UTC-minute filters and an optional coloured keyboard screen. Six focused reader checks brought the unit suite to 65 cases at that stage; it now contains 73. Plain output, file details, keyboard navigation/filtering/refresh and terminal exit were verified against existing output, without restarting the pipeline. The viewer is locally verified, with review still pending.

Work is split into small stages, with a review before moving on. Tests and documentation sit alongside each feature; milestones 4 and 5 extend coverage and check failure behaviour. Monitoring and Agent Forge are optional additions.

| Done | Milestone and scope | Deliverables and acceptance | Tests/checks | Dependencies |
| --- | --- | --- | --- | --- |
| [x] | 0. Specification and architecture | Requirements and local implementation route agreed; later design decisions remain open | Documentation links, sample values, traceability and consistency reviewed | Infrastructure work started on 29 October 2026 |
| [x] | 1. Docker infrastructure and Kafka | Compose setup, readiness checks, topic initialization and run instructions reviewed locally | Startup, repeat initialization, real publish/consume, persistence, shutdown and reset verified | 0 |
| [x] | 2. Producer and raw ingestion | Producer, raw ingester, per-record archive and default-on optional local safeguards reviewed | Core units and four real-Kafka checks pass; Docker output and demo controls verified; blocked writes leave offsets uncommitted | 1; Flink work started on 31 October 2026 |
| [ ] | 3. PyFlink aggregation | DataStream job, DQ, JSONL sink and checkpoints implemented; awaiting review | 59 local units, four Kafka checks and one continuous Kafka/Flink scenario pass: boundaries, ordering, invalid/late input, raw preservation, committed output and quiet windows | 2; baseline and compatible runtime verified |
| [ ] | 4. Unit and end-to-end testing | Concurrent raw/Flink containers, isolated inputs, exact raw/aggregate comparisons and concise reporting; verified, awaiting review | 59 units, four Kafka checks and the concurrent end-to-end scenario pass; readiness, raw byte fidelity, four aggregates, DQ reconciliation, quiet windows and cleanup verified. Broader scenario coverage and replay remain later work | 3; concurrent scenario added on 1 November 2026 |
| [ ] | 5. Reliability and error handling | Recovery, retries and failures behave as documented; core demonstration passes | Interruptions, restart/replay, failed writes, consumer independence and checkpoints if enabled | 4; delivery guarantees agreed |
| [ ] | 6. Prometheus and Grafana: optional | Added ahead of recovery work: bundled Flink reporter, Kafka consumer-lag exporter, optional services and provisioned live dashboard; implemented and verified, awaiting review | Three healthy scrape targets, all 15 panel queries, 742 paced records, DQ exporter counts, committed files and advancing checkpoints verified; consumer lag verified against Kafka offsets and paced traffic; job-name default with optional submission filter; monitoring stops independently; no alerts | Added alongside the core demo; milestone 5 remains deferred |
| [ ] | 7. CI automation | GitHub Actions runs agreed checks and provides useful failure evidence | Clean-runner unit/integration execution; document any exclusions | 5; 6 completed or deferred |
| [ ] | 8. Agent Forge investigation: optional | Isolated archive-limit failure, recorded read-only tools and local AI investigation implemented; awaiting review. DQ defect/recovery remains deferred | Real 3-record capture, 2 archives, blocked offset 2; authenticated tool service; live two-call workflow and no-replay submission verified; default UI incident preset added without another model call; 73 unit cases pass; separate enhanced triage/Condition/User Choice copy checked offline; local Agent Forge file monitor and completion-aware evidence root added, tested without more paid calls; healthy job remains running | Added ahead of CI and recovery; both remain outstanding |
| [ ] | 9. Documentation and final review | Verified README walkthrough, requirement coverage and public-safe source; manual check of public GitHub availability | Full demonstration, required suites, content review and publication check | 7; optional work completed or deferred |

Milestones 1-5 cover the core pipeline and reliability work. The concurrent end-to-end demo and optional monitoring are implemented, but broader scenario coverage, recovery checks, CI and final review are still outstanding.

Before sustained or production use, the archive needs byte-preserving rolled files, defined retention and tested recovery. The current bounded demo has not been verified at sustained daily volume.

CI remains planned and Mermaid diagrams document the design. Additional hardening and alerting remain future work.

Each stage records what changed, what was checked and what remains open. Review and Git changes are handled manually.
