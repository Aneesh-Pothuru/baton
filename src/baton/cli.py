"""BATON command-line interface."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .core import Runtime
from .demo import register_office, run_demo


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="baton")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="run the recorded office")
    demo.add_argument("--database", default="reports/baton.sqlite")
    demo.add_argument("--fixture", default="examples/office.json")
    demo.add_argument("--timeline", default="docs/demo/index.html")

    fire = commands.add_parser("fire", help="fire a registered routine")
    fire.add_argument("routine")
    fire.add_argument("--database", default="reports/routine.sqlite")
    fire.add_argument("--fixture", default="examples/office.json")
    fire.add_argument("--run-id")

    resume = commands.add_parser("resume", help="restore the latest checkpoint")
    resume.add_argument("run_id")
    resume.add_argument("--database", default="reports/baton.sqlite")

    commands.add_parser("clean", help="remove generated local artifacts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "demo":
        summary = run_demo(
            database=args.database,
            fixture_path=args.fixture,
            timeline_path=args.timeline,
        )
        print("BATON Journey 0 complete")
        print(json.dumps(summary, indent=2, sort_keys=True))
        print("Timeline: docs/demo/index.html")
        print("Ledger: reports/baton.sqlite")
        return 0
    if args.command == "fire":
        with Runtime(args.database) as runtime:
            register_office(runtime, args.fixture, "2026-07-24T07:00:00Z")
            run_id = args.run_id or f"{args.routine}-manual"
            runtime.fire_routine(
                args.routine, run_id, "2026-07-24T07:01:00Z"
            )
        print(f"fired {args.routine} as durable run {run_id}")
        return 0
    if args.command == "resume":
        with Runtime(args.database) as runtime:
            restored = runtime.resume(args.run_id, "2026-07-24T09:31:00Z")
        print(json.dumps(restored, indent=2, sort_keys=True))
        return 0
    if args.command == "clean":
        for target in (
            Path("reports/baton.sqlite"),
            Path("reports/routine.sqlite"),
            Path("reports/compounding.json"),
        ):
            if target.exists() and target.is_file():
                target.unlink()
        print("Removed generated BATON artifacts")
        return 0
    return 2

