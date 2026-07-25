from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from baton.core import Runtime
from baton.demo import run_demo


def start_fixture(runtime: Runtime, run_id: str = "run-1") -> None:
    runtime.start_run(
        run_id=run_id,
        agent="agent-1",
        objective="original objective",
        constraints=("keep this verbatim",),
        max_tokens=100,
        max_cost=1,
        max_wall_seconds=100,
        workspace_ref="git:repo@start",
        harness_state={"next": "one"},
        logical_time="2026-07-24T00:00:00Z",
    )


class RuntimeTests(unittest.TestCase):
    def test_resume_restores_three_store_state_after_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "baton.sqlite"
            runtime = Runtime(database)
            start_fixture(runtime)
            runtime.tool_boundary(
                "run-1",
                step=1,
                tool="write",
                result={"ok": True},
                workspace_ref="git:repo@checkpoint",
                harness_state={"next": "two", "value": 4},
                logical_time="2026-07-24T00:01:00Z",
            )
            runtime.close()

            runtime = Runtime(database)
            restored = runtime.resume("run-1", "2026-07-24T00:02:00Z")
            self.assertEqual(restored["step"], 1)
            self.assertEqual(restored["workspace_ref"], "git:repo@checkpoint")
            self.assertEqual(restored["harness_state"], {"next": "two", "value": 4})
            self.assertEqual(restored["handoff"]["constraints"], ["keep this verbatim"])
            runtime.close()

    def test_claims_queue_and_compare_delete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Runtime(Path(directory) / "baton.sqlite") as runtime:
                first = runtime.acquire_claim(
                    "repo:x",
                    "one",
                    ttl_seconds=30,
                    now=1,
                    logical_time="t1",
                )
                second = runtime.acquire_claim(
                    "repo:x",
                    "two",
                    ttl_seconds=30,
                    now=2,
                    logical_time="t2",
                )
                self.assertEqual(first["status"], "GRANTED")
                self.assertEqual(second, {"status": "QUEUED", "position": 1})
                rejected = runtime.release_claim(
                    "repo:x", "stale-token", now=3, logical_time="t3"
                )
                self.assertFalse(rejected["released"])
                released = runtime.release_claim(
                    "repo:x",
                    first["owner_token"],
                    now=4,
                    logical_time="t4",
                )
                self.assertEqual(released["granted_next"]["owner"], "two")
                active = runtime.rows("claims")
                self.assertEqual(len(active), 1)
                self.assertEqual(active[0]["owner"], "two")

    def test_advisory_and_objective_remain_pinned_at_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Runtime(Path(directory) / "baton.sqlite") as runtime:
                start_fixture(runtime)
                runtime.subscribe("run-1", "infra.*")
                runtime.publish_advisory(
                    advisory_id="A-1",
                    topic="infra.deploy",
                    publisher="sre",
                    severity="warn",
                    message="pause deploy",
                    ttl_seconds=100,
                    now=1,
                    logical_time="t1",
                )
                delivered = runtime.deliver_advisories(
                    "run-1", step=1, now=2, logical_time="t2"
                )
                self.assertEqual([item["id"] for item in delivered], ["A-1"])
                runtime.update_objective(
                    "run-1",
                    issuer="human:owner",
                    new_objective="new objective",
                    step=1,
                    logical_time="t3",
                )
                runtime.checkpoint(
                    "run-1",
                    step=1,
                    workspace_ref="git:repo@one",
                    harness_state={"next": "wait"},
                    logical_time="t4",
                )
                restored = runtime.resume("run-1", "t5")
                self.assertEqual(restored["handoff"]["objective"], "new objective")
                self.assertEqual(
                    restored["handoff"]["constraints"], ["keep this verbatim"]
                )
                self.assertEqual(
                    [item["id"] for item in restored["handoff"]["advisories"]],
                    ["A-1"],
                )
                runtime.retract_advisory("A-1", "sre", "t6")
                runtime.deliver_advisories(
                    "run-1", step=2, now=3, logical_time="t7"
                )
                handoff = runtime._handoff("run-1")
                self.assertEqual(json.loads(handoff["advisories"]), [])

    def test_budget_rescope_and_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Runtime(Path(directory) / "baton.sqlite") as runtime:
                start_fixture(runtime)
                self.assertEqual(
                    runtime.record_usage(
                        "run-1",
                        tokens=80,
                        cost=0,
                        wall_seconds=0,
                        logical_time="t1",
                    ),
                    "RESCOPE",
                )
                self.assertEqual(
                    runtime.record_usage(
                        "run-1",
                        tokens=21,
                        cost=0,
                        wall_seconds=0,
                        logical_time="t2",
                    ),
                    "STOP",
                )
                self.assertEqual(runtime._run("run-1")["status"], "PAUSED_BUDGET")

    def test_lesson_gate_promotes_archives_and_abstains(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with Runtime(Path(directory) / "baton.sqlite") as runtime:
                promoted = runtime.gate_lesson(
                    lesson_id="L1",
                    text="good",
                    source_episode_ids=["E1"],
                    baseline_scores=[0.5, 0.6],
                    candidate_scores=[0.7, 0.8],
                    logical_time="t1",
                )
                archived = runtime.gate_lesson(
                    lesson_id="L2",
                    text="bad",
                    source_episode_ids=["E1"],
                    baseline_scores=[0.7],
                    candidate_scores=[0.6],
                    logical_time="t2",
                )
                abstained = runtime.gate_lesson(
                    lesson_id="L3",
                    text="unknown",
                    source_episode_ids=[],
                    baseline_scores=[],
                    candidate_scores=[],
                    logical_time="t3",
                )
                self.assertEqual(promoted["status"], "PROMOTED")
                self.assertEqual(archived["status"], "ARCHIVED")
                self.assertEqual(abstained["status"], "UNDETERMINED")

    def test_journey_zero_artifacts_and_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = run_demo(
                database=root / "baton.sqlite",
                fixture_path=Path(__file__).parents[1] / "examples/office.json",
                timeline_path=root / "docs/demo/index.html",
                pages_index_path=root / "docs/index.html",
                compounding_path=root / "reports/compounding.json",
            )
            self.assertEqual(summary["runs"], 3)
            self.assertEqual(summary["episodes"], 3)
            self.assertEqual(summary["promoted_lessons"], 1)
            self.assertEqual(summary["undetermined_lessons"], 0)
            self.assertEqual(summary["resumed_step"], 2)
            timeline = (root / "docs/demo/index.html").read_text(encoding="utf-8")
            for expected in (
                "docs-agent",
                "deps-agent",
                "sre-agent",
                "CLAIM_QUEUED",
                "ADVISORY_DELIVERED",
                "RUN_RESUMED",
                "LESSON_GATE_EVALUATED",
                'id="scenario"',
                'id="start"',
                'id="pause"',
                'id="step"',
                'id="reset"',
                'id="recover"',
                'href="#main"',
                'aria-pressed="true"',
                "const seedEvents=",
            ):
                self.assertIn(expected, timeline)
            landing = (root / "docs/index.html").read_text(encoding="utf-8")
            architecture = (
                root / "docs/architecture/index.html"
            ).read_text(encoding="utf-8")
            self.assertIn("Agents need an", landing)
            self.assertIn("Inspectable proof", landing)
            self.assertIn('id="about"', landing)
            self.assertIn('href="#main"', landing)
            self.assertIn("State before", architecture)
            self.assertIn("MVP limits", architecture)
            self.assertIn('href="#main"', architecture)


if __name__ == "__main__":
    unittest.main()
