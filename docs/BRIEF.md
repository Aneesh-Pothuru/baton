# 02 · BATON

**An organization of long-running agents: each one durable, each one
remembering and improving across tasks, all of them coordinating —
routines every morning, advisories when something breaks, and no two
agents stepping on the same work.**

`baton` · Python · SQLite/Postgres · A2A-compatible · cron routines

---

## Objective

One long-running agent is a runtime problem. A *team* of them — yours
plus your coworkers' — is an organization problem: shared context,
non-interference, changing objectives, and institutional memory. BATON is
the layer that turns individual agents into a coherent org:

1. **Durable runs.** Every agent survives crashes, restarts, and context
   resets; work is checkpointed and resumable.
2. **Memory that compounds.** Agents store what they learned — episodic
   run history, distilled lessons, promoted skills — and get measurably
   better as the whole fleet launches more tasks. Improvement is
   *eval-gated*: a lesson only gets promoted if it demonstrably helps.
3. **Coordination.** A work registry with leases so agents don't collide;
   an advisory bus so an SRE agent can broadcast "there's a bug in the
   deploy pipeline, route around it" and every affected agent adjusts;
   objective updates that propagate mid-run.
4. **Routines.** Cron-scheduled long-running tasks — "every morning at
   7, reconcile the dashboards, check with the other agents, report" —
   with the memory and coordination layers wired in by default.

One sentence: **BATON is what makes a fleet of long-running agents act
like an organization instead of a crowd.**

---

## Why now

- **Individual durability is table stakes now.** The 2026 runtime
  consensus: separate the model loop from the sandbox from the durable
  session log; checkpoint; treat compaction and context resets as first
  class ([runtime overview](https://slavadubrov.github.io/blog/2026/05/26/ai-agent-runtime/),
  [durable execution](https://zylos.ai/research/2026-04-24-durable-execution-agent-runtimes/)).
  METR's frontier is a ≥16-hour 50%-reliability horizon but only ~3 hours
  at 80% ([Time Horizon 1.1](https://metr.org/blog/2026-1-29-time-horizon-1-1/)) —
  the runtime is what closes that gap in practice.
- **Memory is where the value compounds.** The emerging position: as
  agents learn continually, *their memory becomes more valuable than the
  underlying models* — it's org-specific and compounds
  ([Letta, "Towards Agents That Learn"](https://www.letta.com/blog/towards-agents-that-learn/)).
  The mechanisms are maturing fast: skill libraries as agent-native
  memory (Voyager lineage; [SkillOS](https://arxiv.org/pdf/2605.06614),
  [CODESKILL](https://arxiv.org/pdf/2605.25430)), distilled-lesson stores,
  and a 2026 survey formalizing the write-manage-read loop. Managed
  memory services (Mem0, Letta, Zep) exist — but none of them gate
  memory writes on *measured improvement*, which is the difference
  between learning and accumulating superstition.
- **Coordination has a standard but no fabric.** A2A hit v1.0 under the
  Linux Foundation with 50+ partners
  ([status](https://www.glukhov.org/ai-systems/comparisons/a2a-protocol-2026-adoption/)) —
  agents *can* talk. What's missing is the org layer on top: who's
  working on what, which advisories are active, what changed since
  yesterday. Even the humble lock is subtle — naive stale-lock cleanup
  causes double-runs; you need owner tokens and compare-and-delete
  ([scheduling patterns](https://fast.io/resources/ai-agent-job-scheduling/)).
- **Routines are becoming a product surface** — scheduled agents that
  fire on cron are now first-class in commercial platforms
  ([Claude Managed Agents](https://claude.com/blog/whats-new-in-claude-managed-agents),
  [Azure SRE Agent scheduled tasks](https://learn.microsoft.com/en-us/azure/sre-agent/scheduled-tasks)) —
  but as isolated agents, not as members of a fleet with shared memory
  and advisories.
- **The safety gap is unmeasured.** Context compaction silently erases
  governance constraints and no one measures survival
  ([Governance Decay](https://arxiv.org/pdf/2606.22528)). BATON keeps its
  conformance suite: constraints are verbatim-pinned through resets, and
  survival is *tested*, not assumed.

**The blog post this proves:** "Putting an Agent On-Call," fleet edition —
what happens when the on-call agent can warn the others.

---

## Non-goals

- Not an agent framework — BATON runs your loop (any framework, any
  model) inside its harness; it doesn't tell you how to prompt.
- Not a general workflow engine — Temporal exists; BATON is opinionated
  about agent-specific concerns only (context lifetime, memory,
  coordination, budgets).
- Not a chat platform for agents. Coordination is structured (claims,
  advisories, objective updates) — not free-form agent-to-agent chatter,
  which burns tokens and creates untraceable behavior.
- Not autonomous scope expansion. Agents never *give themselves* new
  objectives; objective changes come from humans or from explicitly
  authorized supervisor agents, and are logged.

---

## Personas

| Persona | Cares about |
|---|---|
| **Agent operator** — runs 3 routines | "Did my morning runs happen, what did they learn, what do I need to see?" |
| **The coworker** — their agents share the repo/infra | "Your agent and mine must not both migrate the same table today." |
| **Fleet owner / platform eng** | "One place to see every agent, every claim, every advisory, every budget." |
| **SRE agent** (a persona that is itself an agent) | "I found a bug; every agent touching the deploy pipeline needs to know *now*." |

---

## User journeys

### Journey 0 — the demo (no API key, <10 minutes)

```bash
git clone …/baton && make demo
```

Spins up a recorded **three-agent office** — `docs-agent` (morning
routine: update docs from merged PRs), `deps-agent` (weekly dependency
bumps), `sre-agent` (watches CI) — and replays a captured morning:
routines fire, `deps-agent` and `docs-agent` both want to touch the same
repo and the work registry serializes them, then CI breaks and
`sre-agent` broadcasts an advisory that pauses `deps-agent`'s pending
merge. The whole morning renders as a **fleet timeline** (static HTML):
every run, claim, advisory, and memory write on one screen. Live mode
runs the same office against Ollama or a free Gemini key.

### J1 — A routine that runs every morning and compounds

Priya registers a routine:

```yaml
# routines/morning-triage.yaml
agent: support-triage
schedule: "0 7 * * 1-5"          # GitHub Actions cron in MVP
objective: triage overnight support inbox; escalate P0s; draft replies
budget: {max_cost: $0.50, max_wall_clock: 45m}
memory: {read: [lessons, skills], write: [episodes]}
coordination: {claims: ["support-inbox"], subscribe: ["infra.*"]}
```

Day 1 the agent takes 40 minutes and mis-routes two tickets. The failures
become episodes; the nightly **distiller** turns them into a candidate
lesson ("billing disputes mentioning 'chargeback' route to finance, not
support"). The lesson is replayed against the last 20 episodes — it would
have fixed 3 mis-routes and broken 0 correct ones — so it's **promoted**.
Day 5, the routine runs in 22 minutes with zero mis-routes. `baton memory
log` shows exactly which lessons exist, their measured win-rate, and
which runs they influenced. Nothing was promoted on vibes.

### J2 — Two agents, one resource, zero collisions

Priya's `deps-agent` and her coworker Sam's `release-agent` both need to
touch `services/api` this morning. Both request a claim:

```
09:02  deps-agent     CLAIM services/api  granted (lease 30m, owner-token a7f3)
09:04  release-agent  CLAIM services/api  queued behind deps-agent
09:31  deps-agent     lease renewed (work in progress)
09:44  deps-agent     RELEASE — release-agent granted
```

Leases have owner tokens and TTLs; a crashed agent's lease expires
cleanly (compare-and-delete, no double-runs). Claims are *advisory
scopes*, not file locks — coarse named resources declared in the routine.
Sam didn't coordinate with Priya; their agents did.

### J3 — The SRE agent broadcasts; the fleet adjusts

At 09:12 `sre-agent`'s routine detects that the deploy pipeline is
failing on a flaky artifact registry. It publishes an advisory:

```
ADVISORY infra.deploy-pipeline  severity=warn  ttl=4h
  "Artifact registry timing out ~30%. Avoid deploys; retry-with-backoff
   if you must. Tracking: INC-4412."
```

Every agent subscribed to `infra.*` receives it at its next decision
point (advisories are injected into context at step boundaries, in the
verbatim-pinned region). `deps-agent` postpones its merge-and-deploy and
says why in its run log. `docs-agent` ignores it — out of scope. When the
SRE agent resolves the incident it retracts the advisory, and the
retraction propagates the same way. At 13:00 Priya reads the timeline:
incident, advisory, three agents' reactions — a legible morning.

### J4 — Objectives change mid-run

Sam's team pivots: the migration `release-agent` is halfway through is
now targeting a different service. Sam updates the objective:

```bash
baton objective update release-agent --run current \
  "Target payments-v2, not payments-v1. Everything already migrated stays."
```

The update lands at the next step boundary as a first-class event: the
agent re-plans against the new objective, records what it kept and
dropped, and the handoff file's objective section is versioned so the
change survives every future context reset. No restart, no lost work.

### End-to-end journey (the product loop)

Register agent → give it a routine → routine fires on schedule → run is
durable (crash = resume) → run writes episodes → distiller proposes
lessons → eval gate promotes the good ones → next runs are better →
agents claim scopes and honor advisories as the fleet grows → the fleet
timeline is the single pane of glass → constraints provably survive
every compaction (conformance suite, run monthly).

---

## PRD

### P0

| ID | Requirement |
|---|---|
| P0-1 | **Durable run core** — append-only step log (SQLite/Postgres); checkpoint at tool-call boundaries; three-store state (workspace in git, harness graph in DB, context as versioned handoff); `baton resume` restores all three. |
| P0-2 | **Structured handoff with verbatim-pinned region** — objective + constraints + active advisories never compressed; handoffs versioned and diffable. |
| P0-3 | **Routines** — cron-scheduled agent runs (GitHub Actions cron or any cron calls `baton fire <routine>`); each firing is a durable run with memory and coordination wired in per the routine spec. |
| P0-4 | **Memory: episodes + lessons** — every run writes an episode (task, trajectory summary, outcome, cost). A distiller proposes lessons; **promotion is eval-gated**: a lesson enters the read-set only if replay against recent episodes shows net improvement. Full provenance: every lesson links to its source episodes and its gate result. |
| P0-5 | **Work registry** — named-scope claims with TTL leases, owner tokens, compare-and-delete release, queueing. `baton claims` shows the live map. |
| P0-6 | **Advisory bus** — publish/subscribe advisories with severity, scope pattern, TTL, and retraction; injected at step boundaries into the pinned context region; every delivery and every agent's reaction logged. |
| P0-7 | **Objective updates** — human-issued (or authorized-supervisor-issued) mid-run objective changes as first-class, versioned events. |
| P0-8 | **Fleet timeline** — static HTML: runs, claims, advisories, memory writes on one time axis. This is Journey 0's artifact. |
| P0-9 | **Budgets** — per-run and per-routine cost/wall-clock/token caps enforced by the runtime, with graceful re-scoping at 80%. |

### P1

| ID | Requirement |
|---|---|
| P1-1 | **Skills promotion** — lessons that recur harden into skills (parameterized procedures / prompt fragments / scripts) with the same eval gate and provenance; skills are shareable across agents and reviewable like code. |
| P1-2 | **Conformance suite** — constraint-survival and goal-drift probes across forced resets (the Governance Decay gap); publish survival numbers per compaction strategy. |
| P1-3 | **A2A endpoint** — each BATON agent exposes an A2A v1.0 agent card; claims/advisories interoperate with non-BATON agents that speak A2A. |
| P1-4 | **Memory hygiene** — lesson decay (unused lessons expire), contradiction detection between lessons, and quarantine: a lesson implicated in a regression is auto-suspended pending re-gate. |
| P1-5 | **Cross-agent memory sharing** — opt-in namespaces: my agent's promoted lessons about `services/api` visible to your agent, with provenance intact. |

### P2

- Supervisor agents: an authorized agent that can issue objective updates
  to a defined set of subordinate agents (the full SRE story), with every
  such update human-visible and revertible.
- Fork-at-checkpoint for counterfactual debugging (CULPRIT's mechanism).
- Hosted multi-tenant mode on Neon/Supabase.

### Success metrics

| Metric | Target |
|---|---|
| Resume correctness (kill -9 at random points, resumed run reaches same terminal state) | ≥ 95% over 100 trials |
| **Compounding**: routine cost & error rate, run 1 vs run 20 on a fixed benchmark routine | improvement demonstrated and published; every gain traceable to named lessons |
| Lesson gate discipline: promoted lessons that later get quarantined | ≤ 10% |
| Claim safety: double-executions of a claimed scope under crash/restart chaos testing | 0 in 1,000 injected-failure trials |
| Advisory latency: publish → visible in subscriber context | ≤ 1 step boundary |
| Constraint survival (verbatim-pinned, 10 forced resets) | 100%; derived-constraint survival measured and published |
| Demo: clone → fleet timeline rendered | < 10 min, $0, no key |

### Launch-day definition

`make demo` three-agent office replay (keyless); live office on
Ollama/Gemini free; routines firing via GitHub Actions cron; memory
promotion demonstrably gated (the day-1-vs-day-5 story reproducible via
`make reproduce-compounding`); claims + advisories working under chaos
tests; fleet timeline; LIMITS.md (single-node MVP, advisory — not
mandatory — claims, no cross-org federation yet).

### Risks

| Risk | Mitigation |
|---|---|
| **Memory poisoning / superstition** — bad lessons compound too | The eval gate is P0, not P1; provenance on every lesson; quarantine on regression; lessons are readable text a human can audit |
| Shared memory leaks context across agents/tenants | Namespaced memory with explicit opt-in sharing; default private |
| Advisory spam becomes noise | Advisories have severity + scope + TTL and cost a publish quota per agent; the timeline makes noisy publishers visible |
| A prompt-injected agent broadcasts a malicious advisory | Advisories carry publisher identity + are injected as *data with provenance*, never as instructions; P0 keeps human-authored objectives supreme; supervisor authority is P2 and permissioned |
| Coordination overhead eats the benefit | Claims are coarse and advisory; measure and publish the overhead (target <8% wall-clock) |
| Distributed-systems rabbit hole | MVP is single-node, one DB; correctness first (owner tokens, leases); federation later via A2A |

---

## System design

```
                       ┌───────────────────────────────────────────┐
   cron (GH Actions) ─▶│              BATON CONTROL PLANE          │
                       │                                           │
                       │  ┌──────────┐  ┌───────────┐  ┌─────────┐ │
                       │  │ ROUTINE  │  │   WORK    │  │ADVISORY │ │
                       │  │ SCHEDULER│  │ REGISTRY  │  │  BUS    │ │
                       │  └────┬─────┘  │ (leases,  │  │(pub/sub,│ │
                       │       │        │  tokens)  │  │  TTL)   │ │
                       │       │        └─────┬─────┘  └────┬────┘ │
                       └───────┼──────────────┼─────────────┼──────┘
                               ▼              │  claims     │ advisories
                  ┌────────────────────┐      │             │
                  │   AGENT RUN (×N)   │◀─────┴─────────────┘
                  │  ┌──────────────┐  │   injected at step boundaries,
                  │  │ your loop    │  │   into the pinned region
                  │  └──────┬───────┘  │
                  │         ▼          │
                  │  step log ─ checkpoints ─ handoffs (durable core) │
                  └─────────┬──────────┘
                            │ episodes
                            ▼
              ┌───────────────────────────┐
              │        MEMORY LAYER       │
              │  episodes ──▶ DISTILLER ──▶ candidate lessons        │
              │                    │                                 │
              │              EVAL GATE  (replay vs recent episodes)  │
              │               pass ▼            fail ▶ archive       │
              │            lessons ──▶ (recurring) ──▶ skills        │
              │            all with provenance + win-rate            │
              └───────────────────────────┘
                            │
                            ▼
              ┌───────────────────────────┐
              │  FLEET TIMELINE (static)  │
              └───────────────────────────┘
```

**Coordination is structured, not conversational.** Three verbs — claim,
advise, update-objective — cover the org problems (non-interference,
shared situational awareness, changing goals) without free-form
agent-to-agent chat. Every verb is logged, attributed, and visible on the
timeline; A2A compatibility (P1) makes the same verbs interoperable with
agents outside BATON.

**The eval gate is the difference between learning and folklore.** A
candidate lesson is replayed against the last N episodes: would runs that
failed have succeeded, would runs that succeeded have broken? Net-positive
lessons promote; everything else archives. Same discipline as FLOTILLA's
kill predicates: the LLM proposes, deterministic evaluation disposes.

**Advisories are data, not commands.** They arrive with publisher
identity and provenance in the pinned region; the agent decides (and
logs) its reaction. This is both the safety posture — a compromised
publisher can't inject instructions — and what keeps reactions auditable.

**MVP is deliberately single-node.** One process, one DB, correct leases.
The org problems this solves don't require distributed systems; they
require *discipline*. Scale-out arrives with the A2A layer.

### Interfaces

- **→ FLOTILLA** (v0.3) — portfolio nodes run as BATON runs.
- **→ TERRARIUM** — a routine can run a TERRARIUM suite nightly; also the
  three-agent demo office can *be* a TERRARIUM world for full simulation.
- **→ CULPRIT** — step logs are CULPRIT's agent-side input; lesson
  provenance is what lets CULPRIT ask "did a promoted memory cause this
  regression?"
- **→ ASSAY** — the lesson eval gate uses ASSAY's scoring interface.
- **loopkit** — Run/trace schemas vendored.

### Milestones

| | Scope |
|---|---|
| **v0.1** | Durable core (step log, checkpoints, resume), routines via cron, budgets, single-agent timeline. **Journey 0 (single-agent) works.** |
| **v0.2** | Work registry + advisory bus + objective updates; three-agent demo office; episodes + distiller + eval-gated lessons. |
| **v0.3** | Fleet timeline polish, memory hygiene, compounding benchmark published, chaos-test results. **Launch.** |
| **v1.0** | Skills, conformance suite numbers, A2A endpoint, cross-agent memory namespaces. |

### Stack & free tier

Python 3.12 · SQLite (Postgres/Neon optional for hosted) · GitPython ·
GitHub Actions cron (free) for routines · LiteLLM (Ollama local for the
demo office; Gemini/Groq free tiers for live) · Docker sandbox per run ·
static HTML timeline on GitHub Pages. The demo office is 3 agents × short
runs — well inside free request quotas. Total required spend: **$0**.
