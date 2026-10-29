# Project instructions

## Scope and workflow

- Read SPEC.md and ROADMAP.md before changes; consult docs/ENGINEERING_GUIDE.md for decisions. Only documentation is authorised until the engineer approves implementation.
- Work in small, reviewable increments within the current milestone. Present unresolved choices for review and record agreed decisions.
- Verify the milestone and obtain engineer review before starting the next. Keep requirements, roadmap and documentation aligned with actual behaviour.

## Code and verification

- Use straightforward, idiomatic Python, small functions, concise descriptive names and useful type hints. Comments explain why. Avoid unnecessary dependencies, abstractions, class hierarchies and repetitive docstrings.
- Preserve the four event fields and original Kafka payloads. Do not silently normalise, enrich or coerce values.
- Use pytest and pragmatic TDD for validation and processing. Cover invalid/missing values, timestamp units and window boundaries.
- Test real Kafka, Flink and file output through isolated scenarios with explicit expected results, bounded observable waits, useful reports and cleanup. Do not duplicate aggregation in tests or rely on arbitrary sleeps.
- Use structured logs and explicit error handling. Review generated changes, failure paths and evidence for reliability claims.

## Documentation and confidentiality

- Use concise British English, Markdown and Mermaid. Avoid em dashes, decorative formatting and marketing claims.
- Distinguish requirements, agreed decisions, proposals and verified capabilities. Keep AI-assisted principles and technical rationale in the engineering guide.
- Use generic terminology and synthetic data. Exclude company names, recruitment context, assessment documents, proprietary material, personal information and secrets from files, configuration and logs.
- If Azure work is later authorised, default to Key Vault Standard with managed identities/references. Review current pricing and expected operations, refreshes, compute, storage, networking and standing charges before provisioning or material changes. This grants no paid-service or broad-permission approval.

## Git and completion

- Do not commit, push, create or merge PRs, or change repository visibility. The engineer owns Git history.
- Done means approved scope delivered, relevant checks passed, documentation/traceability updated and limitations reported. Documentation checks do not establish runtime correctness.
- Report changes, checks, outstanding issues and an accurate short commit suggestion. Do not manufacture history.
- Agent Forge remains deferred and requires separate authorisation.
