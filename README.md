# stream-challenge

A local Python streaming exercise: generate website pageviews, publish them to Kafka, archive the original events and use PyFlink to count pageviews by postcode in one-minute event-time windows. Expected volume is fewer than 100,000 pageviews a day.

## Start here

This repository contains specifications only. No application, Docker setup, tests or CI exist yet. Review [SPEC.md](SPEC.md), then the [design choices](docs/ENGINEERING_GUIDE.md#technical-decisions) and [ROADMAP.md](ROADMAP.md) before approving implementation.

| Task | Current status |
| --- | --- |
| Start the pipeline | Not available until Docker infrastructure and application milestones are complete. |
| Generate events | Not available until the producer is implemented. |
| Run tests | No executable tests yet. Verified commands will be added with each test suite. |
| Inspect outputs | No output files yet. Archive and aggregate formats and paths need agreement. |
| Stop the pipeline | No services have been created. Verified shutdown and data-reset instructions will accompany the Docker setup. |

Only commands tested against the repository will be documented here. Prerequisites and supported versions will be added with the Docker setup.

[ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) contains the proposed architecture, alternatives, testing approach, AI-assisted workflow and future considerations. [AGENTS.md](AGENTS.md) contains actionable rules for coding agents.

Prometheus/Grafana and the later Agent Forge investigation are optional. Neither is implemented.
