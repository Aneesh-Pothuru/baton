# Decisions

## 2026-07-24 — full P0 office before milestone slicing

The milestone table labels v0.1 single-agent while Journey 0 and the P0 table
require the three-agent registry/advisory/memory story. This build follows the
explicit Journey 0 and P0 contract. All behavior remains single-process.

## 2026-07-24 — dependency-free recorded loops

The demo uses recorded deterministic agent actions and standard-library Python.
No live model or Docker dependency is installed. External live paths remain
unclaimed in `LIMITS.md`.

## 2026-07-24 — precise claim semantics

The runtime enforces one active lease per named scope for work it dispatches.
Owner-token compare-and-delete prevents a stale owner releasing a newer lease.
Claims remain advisory outside BATON and are not filesystem locks.

## 2026-07-24 — lesson gate rule

A candidate lesson promotes only when registered candidate replay scores have
positive mean delta over equal-length baseline scores. Missing, mismatched, or
non-finite evidence yields `UNDETERMINED`; non-positive evidence archives.
Source episode IDs, deltas, win rate, and the gate outcome are persisted.

## 2026-07-24 — resume equivalence

The MVP defines checkpoint equivalence as exact restoration of workspace ref,
harness JSON, current step, and the referenced versioned handoff. Real tool
side-effects must be idempotent in the caller; process-level kill testing is a
deferred launch study.

## 2026-07-24 — advisories are data

Advisories enter the pinned handoff as publisher-attributed structured data.
They do not mutate the objective or execute commands. The loop must record its
reaction separately.

