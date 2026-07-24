"""Exercise checkpoint restoration across 100 DB close/reopen boundaries."""

from __future__ import annotations

import tempfile
from pathlib import Path

from baton.core import Runtime


def main() -> int:
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "resume.sqlite"
        passed = 0
        for trial in range(100):
            run_id = f"resume-{trial:03d}"
            runtime = Runtime(database)
            runtime.start_run(
                run_id=run_id,
                agent="fixture-agent",
                objective=f"finish trial {trial}",
                constraints=("retain registered objective",),
                max_tokens=100,
                max_cost=1,
                max_wall_seconds=60,
                workspace_ref=f"git:fixture@{trial}",
                harness_state={"trial": trial, "next": "tool"},
                logical_time="2026-07-24T00:00:00Z",
            )
            runtime.tool_boundary(
                run_id,
                step=1,
                tool="fixture",
                result={"ok": True},
                workspace_ref=f"git:fixture@{trial}-checkpoint",
                harness_state={"trial": trial, "next": "finish"},
                logical_time="2026-07-24T00:00:01Z",
            )
            runtime.close()
            runtime = Runtime(database)
            restored = runtime.resume(run_id, "2026-07-24T00:00:02Z")
            expected = {
                "trial": trial,
                "next": "finish",
            }
            if (
                restored["step"] == 1
                and restored["workspace_ref"]
                == f"git:fixture@{trial}-checkpoint"
                and restored["harness_state"] == expected
                and restored["handoff"]["objective"] == f"finish trial {trial}"
            ):
                passed += 1
            runtime.close()
    assert passed == 100, passed
    print("checkpoint-boundary close/reopen restoration: 100/100")
    print("scope: deterministic SQLite reopen, not random kill -9 chaos")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

