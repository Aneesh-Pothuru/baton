"""Reproduce the registered lesson-gate fixture without inflating its claim."""

from __future__ import annotations

import json

from baton.demo import run_demo


def main() -> int:
    summary = run_demo()
    artifact = json.loads(open("reports/compounding.json", encoding="utf-8").read())
    assert artifact["status"] == "PROMOTED", artifact
    assert artifact["gate_result"] == "PASS", artifact
    assert abs(artifact["mean_delta"] - 0.2) < 1e-9, artifact
    assert artifact["win_rate"] == 1.0, artifact
    assert summary["promoted_lessons"] == 1, summary
    print("reproduced fixture: mean replay delta +0.200000, win rate 1.000000")
    print("scope: registered three-episode fixture; not run-1-vs-run-20 evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

