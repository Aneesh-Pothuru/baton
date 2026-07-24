# Limits

This repository is a compact, honest single-node MVP.

## Demonstrated

- A deterministic three-agent office replay and static fleet timeline.
- SQLite append-only events, tool-boundary checkpoints, and restoration after
  closing and reopening the database connection.
- Versioned pinned handoffs, externally fired routine specs, objective updates,
  budget re-scope/stop signals, queued TTL claims with owner-token release,
  scoped advisories and retraction, episodes, and deterministic lesson gates.
- Gate outcomes include `UNDETERMINED` when replay evidence is missing.

## Not yet demonstrated

- The brief's random `kill -9` ≥95/100 target is unverified. The included
  `make reproduce-resume` exercises 100 deterministic database close/reopen
  trials at declared checkpoint boundaries; it is not an OS-crash study.
- Zero double-executions under 1,000 crash/restart injections is unverified.
  Unit tests cover queueing, expiry, owner-token compare-and-delete, and stale
  release rejection, not process-level chaos.
- The run-1-versus-run-20 compounding benchmark is not established.
  `make reproduce-compounding` reproduces a small registered fixture and proves
  only that net-positive evidence promotes while missing evidence abstains.
- Constraint survival across real model context compaction, derived-constraint
  survival, quarantine rate, coordination overhead, and live advisory latency
  are not measured.
- No model loop, LiteLLM client, Ollama/Gemini live office, Docker sandbox,
  Postgres backend, A2A endpoint, multi-tenant mode, or cross-org federation is
  included.
- Git workspace durability is represented by a recorded immutable workspace
  reference. BATON does not commit, restore, or merge a real working tree.
- Objective authorization is an explicit issuer string in this local runtime;
  production identity, authentication, and supervisor permissions are absent.
- Claims serialize BATON dispatches only. External programs can ignore them.

The deterministic fixture is useful implementation evidence, not a published
reliability or compounding benchmark.

