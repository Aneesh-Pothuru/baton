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
<meta http-equiv="refresh" content="0;url=demo/">
<title>BATON demo</title></head>
<body><p><a href="demo/">Open the BATON fleet timeline</a>.</p></body>
</html>
""",
        encoding="utf-8",
    )
    return summary
