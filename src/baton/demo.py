"""Recorded three-agent office for BATON Journey 0."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .core import RoutineSpec, Runtime, render_timeline


def _load_specs(path: str | Path) -> list[RoutineSpec]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        RoutineSpec(
            name=item["name"],
            agent=item["agent"],
            schedule=item["schedule"],
            objective=item["objective"],
            constraints=tuple(item["constraints"]),
            max_tokens=int(item["budget"]["max_tokens"]),
            max_cost=float(item["budget"]["max_cost"]),
            max_wall_seconds=int(item["budget"]["max_wall_seconds"]),
            claims=tuple(item["coordination"]["claims"]),
            subscriptions=tuple(item["coordination"]["subscribe"]),
            memory_read=tuple(item["memory"]["read"]),
            memory_write=tuple(item["memory"]["write"]),
            workspace_ref=item["workspace_ref"],
            harness_state=item["harness_state"],
        )
        for item in raw["routines"]
    ]


def register_office(
    runtime: Runtime,
    fixture_path: str | Path,
    logical_time: str = "2026-07-24T08:55:00Z",
) -> list[RoutineSpec]:
    specs = _load_specs(fixture_path)
    for spec in specs:
        runtime.register_routine(spec, logical_time)
    return specs


def run_demo(
    *,
    database: str | Path = "reports/baton.sqlite",
    fixture_path: str | Path = "examples/office.json",
    timeline_path: str | Path = "docs/demo/index.html",
    pages_index_path: str | Path = "docs/index.html",
    compounding_path: str | Path = "reports/compounding.json",
) -> dict[str, Any]:
    database_target = Path(database)
    if database_target.exists():
        database_target.unlink()
    runtime = Runtime(database_target)
    specs = register_office(runtime, fixture_path)
    by_agent = {spec.agent: spec for spec in specs}

    docs_run = runtime.fire_routine(
        by_agent["docs-agent"].name,
        "run-docs-20260724",
        "2026-07-24T09:00:00Z",
    )
    deps_run = runtime.fire_routine(
        by_agent["deps-agent"].name,
        "run-deps-20260724",
        "2026-07-24T09:01:00Z",
    )
    sre_run = runtime.fire_routine(
        by_agent["sre-agent"].name,
        "run-sre-20260724",
        "2026-07-24T09:02:00Z",
    )

    docs_claim = runtime.acquire_claim(
        "repo:portfolio",
        "docs-agent",
        ttl_seconds=1800,
        now=100,
        logical_time="2026-07-24T09:03:00Z",
    )
    runtime.acquire_claim(
        "repo:portfolio",
        "deps-agent",
        ttl_seconds=1800,
        now=101,
        logical_time="2026-07-24T09:04:00Z",
    )
    runtime.tool_boundary(
        docs_run,
        step=1,
        tool="inspect-merged-prs",
        result={"merged_prs": 2},
        workspace_ref="git:portfolio@demo-before-docs",
        harness_state={"next": "write-docs", "merged_prs": [41, 42]},
        logical_time="2026-07-24T09:06:00Z",
    )
    runtime.tool_boundary(
        deps_run,
        step=1,
        tool="inspect-dependencies",
        result={"updates": 3, "claim": "queued"},
        workspace_ref="git:portfolio@demo-before-deps",
        harness_state={"next": "wait-for-claim", "updates": 3},
        logical_time="2026-07-24T09:07:00Z",
    )
    runtime.record_usage(
        docs_run,
        tokens=900,
        cost=0.0,
        wall_seconds=120,
        logical_time="2026-07-24T09:08:00Z",
    )
    runtime.record_usage(
        deps_run,
        tokens=8100,
        cost=0.0,
        wall_seconds=240,
        logical_time="2026-07-24T09:09:00Z",
    )
    runtime.publish_advisory(
        advisory_id="ADV-4412",
        topic="infra.deploy-pipeline",
        publisher="sre-agent",
        severity="warn",
        message=(
            "Artifact registry timing out in the recorded fixture; avoid deploys "
            "and retain provenance."
        ),
        ttl_seconds=14400,
        now=200,
        logical_time="2026-07-24T09:12:00Z",
    )
    release = runtime.release_claim(
        "repo:portfolio",
        str(docs_claim["owner_token"]),
        now=205,
        logical_time="2026-07-24T09:14:00Z",
    )
    if release["granted_next"]["owner"] != "deps-agent":
        raise AssertionError("queued dependency routine did not receive claim")
    deps_claim_token = str(release["granted_next"]["owner_token"])

    delivered = runtime.deliver_advisories(
        deps_run,
        step=2,
        now=206,
        logical_time="2026-07-24T09:15:00Z",
    )
    if [item["id"] for item in delivered] != ["ADV-4412"]:
        raise AssertionError("advisory was not delivered at the next boundary")
    runtime.record_advisory_reaction(
        deps_run,
        "ADV-4412",
        "Paused merge-and-deploy; dependency patch remains staged.",
        "2026-07-24T09:15:30Z",
    )
    runtime.update_objective(
        deps_run,
        issuer="human:priya",
        new_objective=(
            "Stage the safe dependency patch, but do not merge or deploy until "
            "ADV-4412 is retracted."
        ),
        step=2,
        logical_time="2026-07-24T09:16:00Z",
    )
    runtime.tool_boundary(
        deps_run,
        step=2,
        tool="stage-dependency-patch",
        result={"staged": True, "merged": False},
        workspace_ref="git:portfolio@demo-deps-staged",
        harness_state={"next": "wait-for-advisory-retraction", "patch": "deps-v2"},
        logical_time="2026-07-24T09:17:00Z",
    )

    runtime.write_episode(
        episode_id="E-DOCS-01",
        run_id=docs_run,
        task="Update docs from merged PRs",
        trajectory_summary="Inspected two PRs and updated release notes.",
        outcome="success",
        cost=0.0,
        logical_time="2026-07-24T09:20:00Z",
    )
    runtime.write_episode(
        episode_id="E-DEPS-01",
        run_id=deps_run,
        task="Prepare dependency bumps",
        trajectory_summary="Staged patch and paused merge after advisory.",
        outcome="safe-pause",
        cost=0.0,
        logical_time="2026-07-24T09:21:00Z",
    )
    runtime.write_episode(
        episode_id="E-SRE-01",
        run_id=sre_run,
        task="Watch CI",
        trajectory_summary="Detected fixture incident and published scoped advisory.",
        outcome="advisory-published",
        cost=0.0,
        logical_time="2026-07-24T09:22:00Z",
    )
    gate = runtime.gate_lesson(
        lesson_id="L-PAUSE-DEPLOY",
        text="When a scoped deploy advisory is active, stage safe work but defer merge.",
        source_episode_ids=["E-DOCS-01", "E-DEPS-01", "E-SRE-01"],
        baseline_scores=[0.50, 0.55, 0.60],
        candidate_scores=[0.70, 0.75, 0.80],
        logical_time="2026-07-24T09:25:00Z",
    )
    if gate["status"] != "PROMOTED":
        raise AssertionError("registered net-positive lesson was not promoted")
    runtime.complete_run(
        docs_run, "docs updated and claim released", "2026-07-24T09:26:00Z"
    )
    runtime.complete_run(
        sre_run, "incident advisory remains active", "2026-07-24T09:27:00Z"
    )

    runtime.event(
        "2026-07-24T09:30:00Z",
        "RUN_INTERRUPTED",
        run_id=deps_run,
        actor="recorded-office",
        payload={"kind": "database-connection-close"},
    )
    runtime.close()

    runtime = Runtime(database_target)
    restored = runtime.resume(deps_run, "2026-07-24T09:31:00Z")
    if restored["workspace_ref"] != "git:portfolio@demo-deps-staged":
        raise AssertionError("workspace reference did not resume")
    if restored["handoff"]["objective"].startswith("Stage the safe") is False:
        raise AssertionError("updated objective was not pinned in resumed handoff")
    if [item["id"] for item in restored["handoff"]["advisories"]] != ["ADV-4412"]:
        raise AssertionError("active advisory did not survive resume")

    runtime.retract_advisory(
        "ADV-4412", "sre-agent", "2026-07-24T09:40:00Z"
    )
    runtime.deliver_advisories(
        deps_run,
        step=3,
        now=400,
        logical_time="2026-07-24T09:41:00Z",
    )
    runtime.tool_boundary(
        deps_run,
        step=3,
        tool="complete-merge",
        result={"merged": True, "deployed": False},
        workspace_ref="git:portfolio@demo-deps-merged",
        harness_state={"next": "report", "patch": "deps-v2"},
        logical_time="2026-07-24T09:42:00Z",
    )
    runtime.release_claim(
        "repo:portfolio",
        deps_claim_token,
        now=402,
        logical_time="2026-07-24T09:43:00Z",
    )
    runtime.complete_run(
        deps_run,
        "patch merged after advisory retraction; deploy intentionally omitted",
        "2026-07-24T09:44:00Z",
    )

    compounding = {
        "schema_version": "baton.compounding-fixture.v1",
        "lesson_id": gate["lesson_id"],
        "source_episodes": gate["source_episodes"],
        "baseline_scores": [0.50, 0.55, 0.60],
        "candidate_scores": [0.70, 0.75, 0.80],
        "mean_delta": gate["mean_delta"],
        "win_rate": gate["win_rate"],
        "gate_result": gate["gate_result"],
        "status": gate["status"],
        "claim_scope": "registered fixture only; not run-1-vs-run-20 evidence"
    }
    compounding_target = Path(compounding_path)
    compounding_target.parent.mkdir(parents=True, exist_ok=True)
    compounding_target.write_text(
        json.dumps(compounding, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    render_timeline(runtime, timeline_path)
    summary = {
        "runs": len(runtime.rows("runs")),
        "events": len(runtime.rows("events")),
        "claims_active": len(runtime.rows("claims")),
        "claims_queued": len(runtime.rows("claim_queue")),
        "advisories": len(runtime.rows("advisories")),
        "episodes": len(runtime.rows("episodes")),
        "promoted_lessons": sum(
            row["status"] == "PROMOTED" for row in runtime.rows("lessons")
        ),
        "undetermined_lessons": sum(
            row["status"] == "UNDETERMINED" for row in runtime.rows("lessons")
        ),
        "resumed_step": restored["step"],
        "resumed_handoff_version": restored["handoff"]["version"],
    }
    runtime.close()

    pages_target = Path(pages_index_path)
    pages_target.parent.mkdir(parents=True, exist_ok=True)
    pages_target.write_text(
        """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="BATON is a durable organization runtime for
coordinated agent routines, recoverable handoffs, and eval-gated memory.">
<meta name="color-scheme" content="light">
<link rel="stylesheet" href="assets/site.css">
<title>BATON — a durable organization for agents</title></head>
<body><a class="skip-link" href="#main">Skip to content</a>
<header class="site-nav"><a class="brand" href="./">BATON
<span>organization runtime</span></a><nav aria-label="Main navigation">
<a href="#thesis">Thesis</a><a href="#evidence">Evidence</a>
<a href="#about">About</a>
<a href="architecture/">Architecture</a>
<a href="https://github.com/Aneesh-Pothuru/baton">GitHub</a>
<a class="score-link" href="demo/">Open the score →</a></nav></header>
<main id="main"><section class="hero"><div><p class="eyebrow">Durable coordination /
recorded office 01</p><h1>Agents need an <em>organization.</em></h1>
<p class="hero-copy">BATON turns long-running agent loops into a legible
institution: scheduled routines, leased claims, scoped advisories, pinned
objectives, recoverable checkpoints, and memory that must earn promotion.</p>
<div class="actions"><a class="button primary" href="demo/">Conduct the live
replay</a><a class="button ghost" href="#evidence">Inspect the proof</a></div>
</div><aside class="cover-note"><span class="proof-label">Program note</span>
<p class="quote">A living office should read like a score—not a pile of
chat logs.</p><ul class="cover-facts"><li><span>Ensemble</span>
<strong>3 agents</strong></li><li><span>Record</span>
<strong>42 events</strong></li><li><span>Persistence</span>
<strong>SQLite</strong></li><li><span>Model key</span>
<strong>Not required</strong></li></ul></aside></section>
<section class="proof-ribbon" aria-label="Recorded implementation proof">
<div class="proof"><strong>03</strong><span>durable agent routines</span></div>
<div class="proof"><strong>42</strong><span>append-only source events</span></div>
<div class="proof"><strong>01</strong><span>real close / reopen recovery</span></div>
<div class="proof"><strong>0</strong><span>API keys required to reproduce</span></div>
</section>
<section class="section" id="thesis"><div class="section-heading"><div>
<p class="section-index">I / The organizational gap</p>
<h2>Intelligence is not coordination.</h2></div>
<p>Agent frameworks help a model act. BATON focuses on the institutional layer
around those acts: who owns work, what changed, which warning applies, what
survives a restart, and which lessons deserve to become policy.</p></div>
<div class="problem-grid"><article class="problem"><b>01</b>
<h3>Work collides.</h3><p>TTL claims and FIFO contention replace polite
assumptions with explicit, inspectable ownership.</p></article>
<article class="problem"><b>02</b><h3>Context drifts.</h3>
<p>Versioned handoffs pin objectives, constraints, and delivered advisories at
tool boundaries.</p></article><article class="problem"><b>03</b>
<h3>Memory lies.</h3><p>Episodes can suggest lessons, but evaluation decides
whether they are promoted, archived, or left undetermined.</p></article></div>
</section>
<section class="section"><div class="section-heading"><div>
<p class="section-index">II / The control surface</p>
<h2>Read the office across time.</h2></div>
<p>The primary interface borrows the grammar of an orchestral score. Each actor
holds a lane; claims, advisories, checkpoints, and memory become distinct cues
on one shared axis. Nothing is collapsed into a vague “agent activity” feed.</p>
</div><div class="score-preview" aria-label="Preview of the BATON score">
<div class="preview-top"><span>Recorded morning / logical time →</span>
<strong>Replay complete</strong></div>
<div class="preview-row"><div class="preview-label">docs-agent
<small>claim holder</small></div><div class="staff"><i class="note"></i>
<i class="note claim"></i><i class="note hollow"></i>
<i class="note memory"></i></div></div>
<div class="preview-row"><div class="preview-label">deps-agent
<small>recoverable run</small></div><div class="staff">
<i class="note"></i><i class="note claim"></i>
<i class="note warning"></i><i class="note hollow"></i>
<i class="note memory"></i></div></div>
<div class="preview-row"><div class="preview-label">sre-agent
<small>advisory publisher</small></div><div class="staff">
<i class="note"></i><i class="note warning"></i>
<i class="note memory"></i></div></div></div>
<div class="journey" aria-label="BATON coordination loop">
<article class="act"><strong>01</strong><h3>Schedule</h3>
<p>A routine creates a versioned objective and budget envelope.</p></article>
<article class="act"><strong>02</strong><h3>Claim</h3>
<p>Work scopes have one owner token and a deterministic queue.</p></article>
<article class="act"><strong>03</strong><h3>Conduct</h3>
<p>Scoped advisories arrive only at explicit step boundaries.</p></article>
<article class="act"><strong>04</strong><h3>Recover</h3>
<p>Workspace and harness state resume from the last durable checkpoint.</p></article>
<article class="act"><strong>05</strong><h3>Learn</h3>
<p>Only evidence-backed lessons move into durable organizational memory.</p>
</article></div></section>
<section class="evidence-band" id="evidence"><div class="evidence">
<p class="section-index">III / Inspectable proof</p>
<h2>The interface leads back to artifacts.</h2>
<div class="evidence-grid"><article class="evidence-item">
<b>Coordination</b><h3>Claim queue, not choreography.</h3>
<p>The fixture proves one holder, one queued contender, token-checked release,
and automatic transfer on release.</p></article>
<article class="evidence-item"><b>Advisory delivery</b>
<h3>Warnings become pinned data.</h3><p>A scoped SRE advisory reaches the
dependency agent at its next boundary and survives restart in the handoff.</p>
</article><article class="evidence-item"><b>Recovery</b>
<h3>The process actually closes.</h3><p>The recorded run closes its SQLite
connection, reopens it, restores step 2, then continues. The
<code>make reproduce-resume</code> harness exercises 100 cycles.</p></article>
<article class="evidence-item"><b>Memory hygiene</b>
<h3>Promotion is an evaluated event.</h3><p>Candidate scores beat their
registered baselines before the fixture lesson is marked promoted. See the
<a href="demo/">complete event transcript</a>.</p></article></div></div></section>
<section class="architecture"><div class="section-heading"><div>
<p class="section-index">IV / Architecture</p>
<h2>One compact, durable spine.</h2></div><p>The MVP stays deliberately small:
standard-library Python and SQLite, no model SDK, no server needed for the
published evidence. The runtime is the product; the score is its projection.</p>
</div><div class="architecture-flow" aria-label="BATON architecture flow">
<article class="architecture-node"><span>01 / INPUT</span><h3>Routine spec</h3>
<p>Objective, schedule, budgets, claims, subscriptions, and workspace ref.</p>
</article><article class="architecture-node"><span>02 / STATE</span>
<h3>Versioned handoff</h3><p>Verbatim objective, constraints, and advisory
set.</p></article><article class="architecture-node"><span>03 / BOUNDARY</span>
<h3>Tool checkpoint</h3><p>Step, workspace reference, and harness state commit
together.</p></article><article class="architecture-node"><span>04 / RECORD</span>
<h3>Event ledger</h3><p>Every organizational transition enters one ordered
SQLite record.</p></article><article class="architecture-node">
<span>05 / PROJECTION</span><h3>Conductor score</h3>
<p>Static Pages turns that record into an interactive replay.</p></article>
</div><div class="actions"><a class="button ghost" href="architecture/">
Read the architecture and limits</a></div></section>
<section class="boundary" id="about"><div>
<p class="section-index">V / About &amp; honest boundary</p>
<h2>Small enough to inspect.</h2></div><ul class="boundary-list">
<li><strong>It is</strong><span>A single-node runtime harness for deterministic,
durable agent routines.</span></li><li><strong>It is not</strong>
<span>A distributed scheduler, agent framework, OS lock manager, or reliability
benchmark.</span></li><li><strong>Measured</strong><span>Journey 0 behavior,
event order, recovery state, and the registered memory-gate fixture.</span></li>
<li><strong>Unmeasured</strong><span>Fleet-scale throughput, cross-node
consensus, and real-model quality.</span></li></ul></section>
<section class="cta"><p class="section-index">The full score is interactive</p>
<h2>Conduct, pause, step, filter, and replay recovery.</h2>
<a class="button" href="demo/">Enter the recorded office →</a></section></main>
<footer class="site-footer"><span>Fixture-derived implementation evidence,
not a reliability benchmark.</span><span><a href="architecture/">Architecture</a>
· <a href="https://github.com/Aneesh-Pothuru/baton">Source on GitHub</a></span>
</footer></body>
</html>
""",
        encoding="utf-8",
    )
    architecture_target = pages_target.parent / "architecture" / "index.html"
    architecture_target.parent.mkdir(parents=True, exist_ok=True)
    architecture_target.write_text(
        """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="How BATON makes coordinated agent routines
durable, recoverable, and inspectable.">
<meta name="color-scheme" content="light">
<link rel="stylesheet" href="../assets/site.css">
<title>BATON — architecture and limits</title></head>
<body class="architecture-page"><a class="skip-link" href="#main">
Skip to architecture</a><header class="site-nav">
<a class="brand" href="../">BATON <span>architecture score</span></a>
<nav aria-label="Main navigation"><a href="../#thesis">Thesis</a>
<a href="../#evidence">Evidence</a>
<a href="https://github.com/Aneesh-Pothuru/baton">GitHub</a>
<a class="score-link" href="../demo/">Open the score →</a></nav></header>
<main id="main"><section class="hero"><div><p class="eyebrow">System score /
five durable movements</p><h1>State before <em>spectacle.</em></h1>
<p class="hero-copy">BATON’s architecture is a compact organizational kernel.
It records coordination decisions before visualizing them, and recovery is a
normal state transition rather than an exceptional UI path.</p>
<div class="actions"><a class="button primary" href="../demo/">Replay the
implementation</a><a class="button ghost"
href="https://github.com/Aneesh-Pothuru/baton">Read the source</a></div></div>
<aside class="cover-note"><span class="proof-label">Durability contract</span>
<p class="quote">If a decision cannot survive restart, it is not organizational
memory.</p><ul class="cover-facts"><li><span>Storage</span>
<strong>SQLite / WAL-safe commits</strong></li><li><span>Ordering</span>
<strong>Monotonic sequence</strong></li><li><span>Handoffs</span>
<strong>Versioned / pinned</strong></li><li><span>Projection</span>
<strong>Static / keyless</strong></li></ul></aside></section>
<section class="section"><div class="section-heading"><div>
<p class="section-index">I / Runtime topology</p>
<h2>Five layers. One record.</h2></div><p>Each layer has one narrow job, so an
operator can follow an objective from scheduled intent to durable evidence
without reverse-engineering a framework’s internal graph.</p></div>
<div class="architecture-flow"><article class="architecture-node">
<span>01 / ROUTINE</span><h3>Declare</h3><p>Schedule, objective, constraints,
budget, coordination, memory, workspace.</p></article>
<article class="architecture-node"><span>02 / HANDOFF</span><h3>Pin</h3>
<p>Version the exact instructions and active advisories read by the run.</p>
</article><article class="architecture-node"><span>03 / CLAIM</span>
<h3>Coordinate</h3><p>Lease a scope with an opaque owner token or join its
FIFO queue.</p></article><article class="architecture-node">
<span>04 / CHECKPOINT</span><h3>Recover</h3><p>Commit step, workspace ref,
harness state, and handoff version.</p></article>
<article class="architecture-node"><span>05 / EVENT</span><h3>Explain</h3>
<p>Append the transition and render it into the shared organizational score.</p>
</article></div></section>
<section class="section"><div class="section-heading"><div>
<p class="section-index">II / Operational contracts</p>
<h2>The boundaries are the interface.</h2></div><p>The prototype makes its
consistency points explicit. That keeps claims and recovery testable with no
external service and makes omissions visible.</p></div><div class="detail-grid">
<article class="detail"><p class="section-index">Claims</p>
<h3>Compare-and-delete release.</h3><p>Only the opaque owner token can release
a live claim. Expiry or valid release grants the next queued contender in
deterministic order.</p></article><article class="detail">
<p class="section-index">Advisories</p><h3>Data, never hidden commands.</h3>
<p>Topic-scoped advisories carry publisher, severity, TTL, and provenance.
Agents receive them only at a step boundary and record their reaction.</p>
</article><article class="detail"><p class="section-index">Recovery</p>
<h3>Resume from the durable edge.</h3><p><code>Runtime.resume</code> restores
the latest checkpoint and its exact handoff. Work after that edge must be
re-executed, never imagined complete.</p></article><article class="detail">
<p class="section-index">Memory</p><h3>Abstention is a valid verdict.</h3>
<p>Invalid or missing comparison evidence yields <code>UNDETERMINED</code>.
Positive registered deltas promote; non-positive evidence archives.</p>
</article></div></section>
<section class="boundary"><div><p class="section-index">III / MVP limits</p>
<h2>What this score does not claim.</h2></div><ul class="boundary-list">
<li><strong>Single node</strong><span>SQLite coordination is process-local;
there is no distributed consensus or cross-host lease service.</span></li>
<li><strong>Harness scope</strong><span>Claims cover BATON-dispatched work,
not arbitrary operating-system writes.</span></li>
<li><strong>Recorded agents</strong><span>The demo is deterministic and ships
no model SDK, provider account, or quality claim.</span></li>
<li><strong>Fixture eval</strong><span>The promoted lesson proves gate behavior
for registered scores, not compounding from run 1 to run 20.</span></li>
<li><strong>Static UI</strong><span>Scenario controls are client-side
projections; the source office remains the only measured replay.</span></li>
</ul></section><section class="cta"><p class="section-index">See the state move</p>
<h2>Replay every contract on the conductor score.</h2>
<a class="button" href="../demo/">Open interactive Journey 0 →</a></section>
</main><footer class="site-footer"><span>Architecture derived from the shipped
single-node implementation.</span><span><a href="../">Overview</a> ·
<a href="https://github.com/Aneesh-Pothuru/baton">Source on GitHub</a></span>
</footer></body></html>
""",
        encoding="utf-8",
    )
    return summary
