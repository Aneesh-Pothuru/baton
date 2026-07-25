from __future__ import annotations

import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from baton.config import ServiceConfig
from baton.server import create_server


ROOT = Path(__file__).parents[1]


class RunningServer:
    def __init__(self, config: ServiceConfig):
        self.server = create_server(config)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.base = f"http://{host}:{port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        token: str | None = None,
        origin: str | None = None,
        method: str | None = None,
    ) -> tuple[int, dict[str, object] | str]:
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if data else {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if origin:
            headers["Origin"] = origin
        request = Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urlopen(request, timeout=3) as response:
                content = response.read()
                if "json" in response.headers.get("Content-Type", ""):
                    return response.status, json.loads(content)
                return response.status, content.decode()
        except HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read())


class ServiceTests(unittest.TestCase):
    def test_end_to_end_control_plane_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "baton.sqlite"
            server = RunningServer(
                ServiceConfig(
                    database=database,
                    port=0,
                    static_dir=ROOT / "docs",
                )
            )
            try:
                status, health = server.request("/healthz")
                self.assertEqual(status, 200)
                self.assertEqual(health["status"], "ok")
                status, ready = server.request("/readyz")
                self.assertEqual(status, 200)
                self.assertEqual(ready["status"], "ready")

                routine = {
                    "name": "release-watch",
                    "agent": "release-agent",
                    "schedule": "0 7 * * 1-5",
                    "objective": "Prepare a release without bypassing CI.",
                    "constraints": ["Never deploy without approval."],
                    "budget": {
                        "max_tokens": 2000,
                        "max_cost": 1,
                        "max_wall_seconds": 900,
                    },
                    "coordination": {
                        "claims": ["repo:api"],
                        "subscribe": ["infra.*"],
                    },
                    "workspace_ref": "git:api@main",
                    "harness_state": {"next": "inspect-ci"},
                }
                status, registered = server.request(
                    "/api/v1/routines", body=routine
                )
                self.assertEqual(status, 201)
                self.assertEqual(registered["data"]["routine"], "release-watch")

                status, fired = server.request(
                    "/api/v1/routines/release-watch/fire",
                    body={"run_id": "run-release-1", "now": 100},
                )
                self.assertEqual(status, 201)
                self.assertEqual(fired["data"]["claims"][0]["status"], "GRANTED")

                status, claims = server.request("/api/v1/claims")
                self.assertEqual(status, 200)
                self.assertEqual(claims["data"]["active"][0]["scope"], "repo:api")

                status, advisory = server.request(
                    "/api/v1/advisories",
                    body={
                        "id": "ADV-1",
                        "topic": "infra.deploy",
                        "publisher": "sre-agent",
                        "severity": "warn",
                        "message": "Pause deploys while CI is degraded.",
                        "ttl_seconds": 300,
                        "now": 101,
                    },
                )
                self.assertEqual(status, 201)
                self.assertEqual(advisory["data"]["advisory"], "ADV-1")

                status, delivered = server.request(
                    "/api/v1/runs/run-release-1/deliver-advisories",
                    body={"step": 1, "now": 102},
                )
                self.assertEqual(status, 200)
                self.assertEqual(delivered["data"]["delivered"][0]["id"], "ADV-1")
                status, reaction = server.request(
                    "/api/v1/runs/run-release-1/advisory-reaction",
                    body={
                        "advisory_id": "ADV-1",
                        "reaction": "Staged the release and paused deployment.",
                    },
                )
                self.assertEqual(status, 200)
                self.assertTrue(reaction["data"]["recorded"])

                status, objective = server.request(
                    "/api/v1/runs/run-release-1/objective",
                    body={
                        "issuer": "human:owner",
                        "objective": "Stage the release; do not deploy.",
                        "step": 1,
                    },
                )
                self.assertEqual(status, 200)
                self.assertEqual(objective["data"]["handoff_version"], 3)

                status, checkpoint = server.request(
                    "/api/v1/runs/run-release-1/tool-boundary",
                    body={
                        "step": 1,
                        "tool": "inspect-ci",
                        "result": {"passing": False},
                        "workspace_ref": "git:api@staged",
                        "harness_state": {"next": "wait-for-ci"},
                    },
                )
                self.assertEqual(status, 200)
                self.assertEqual(checkpoint["data"]["checkpointed_step"], 1)

                status, episode = server.request(
                    "/api/v1/episodes",
                    body={
                        "id": "EP-1",
                        "run_id": "run-release-1",
                        "task": "Prepare release",
                        "trajectory_summary": "Inspected CI and paused safely.",
                        "outcome": "safe-pause",
                        "cost": 0,
                    },
                )
                self.assertEqual(status, 201)
                self.assertEqual(episode["data"]["episode"], "EP-1")
                status, lesson = server.request(
                    "/api/v1/lessons/gate",
                    body={
                        "id": "LESSON-1",
                        "text": "Pause a release when its scoped advisory is active.",
                        "source_episode_ids": ["EP-1"],
                        "baseline_scores": [0.4],
                        "candidate_scores": [0.8],
                    },
                )
                self.assertEqual(status, 201)
                self.assertEqual(lesson["data"]["status"], "PROMOTED")

                status, evidence = server.request(
                    "/api/v1/runs/run-release-1/evidence"
                )
                self.assertEqual(status, 200)
                self.assertEqual(
                    evidence["data"]["run"]["handoff"]["objective"],
                    "Stage the release; do not deploy.",
                )
                self.assertGreaterEqual(len(evidence["data"]["events"]), 9)

                status, demo = server.request("/demo/")
                self.assertEqual(status, 200)
                self.assertIn("Journey 0", demo)
            finally:
                server.close()

            restarted = RunningServer(ServiceConfig(database=database, port=0))
            try:
                status, run = restarted.request("/api/v1/runs/run-release-1")
                self.assertEqual(status, 200)
                self.assertEqual(
                    run["data"]["checkpoint"]["workspace_ref"],
                    "git:api@staged",
                )
                status, resumed = restarted.request(
                    "/api/v1/runs/run-release-1/resume", body={}
                )
                self.assertEqual(status, 200)
                self.assertEqual(resumed["data"]["step"], 1)
                self.assertEqual(
                    resumed["data"]["harness_state"]["next"],
                    "wait-for-ci",
                )
            finally:
                restarted.close()

    def test_token_and_loopback_safety_defaults(self) -> None:
        with self.assertRaisesRegex(ValueError, "BATON_API_TOKEN"):
            ServiceConfig(host="0.0.0.0", port=0).validate()
        with tempfile.TemporaryDirectory() as directory:
            server = RunningServer(
                ServiceConfig(
                    database=Path(directory) / "baton.sqlite",
                    port=0,
                    api_token="test-secret",
                )
            )
            try:
                status, denied = server.request("/api/v1/status")
                self.assertEqual(status, 401)
                self.assertEqual(denied["error"]["code"], "unauthorized")
                status, allowed = server.request(
                    "/api/v1/status",
                    token="test-secret",
                    origin=server.base,
                )
                self.assertEqual(status, 200)
                self.assertEqual(allowed["data"]["service"], "baton")
                status, denied_origin = server.request(
                    "/api/v1/status",
                    token="test-secret",
                    origin="https://malicious.example",
                )
                self.assertEqual(status, 403)
                self.assertEqual(
                    denied_origin["error"]["code"], "origin_denied"
                )
                status, _ = server.request(
                    "/api/v1/status",
                    origin=server.base,
                    method="OPTIONS",
                )
                self.assertEqual(status, 204)
            finally:
                server.close()

    def test_invalid_requests_return_honest_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            server = RunningServer(
                ServiceConfig(
                    database=Path(directory) / "baton.sqlite",
                    port=0,
                )
            )
            try:
                status, missing = server.request("/api/v1/runs/missing")
                self.assertEqual(status, 404)
                self.assertEqual(missing["error"]["code"], "not_found")
                status, invalid = server.request(
                    "/api/v1/routines", body={"name": "incomplete"}
                )
                self.assertEqual(status, 422)
                self.assertEqual(invalid["error"]["code"], "invalid_request")
            finally:
                server.close()

    def test_concurrent_claim_requests_are_serialized_into_owner_and_queue(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            server = RunningServer(
                ServiceConfig(
                    database=Path(directory) / "baton.sqlite",
                    port=0,
                )
            )
            try:
                def acquire(owner: str) -> tuple[int, dict[str, object] | str]:
                    return server.request(
                        "/api/v1/claims",
                        body={
                            "scope": "repo:shared",
                            "owner": owner,
                            "ttl_seconds": 300,
                            "now": 10,
                        },
                    )

                with ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(acquire, ("agent-a", "agent-b")))
                self.assertEqual([status for status, _ in results], [201, 201])
                states = sorted(
                    result["data"]["status"] for _, result in results
                )
                self.assertEqual(states, ["GRANTED", "QUEUED"])
                status, claims = server.request("/api/v1/claims")
                self.assertEqual(status, 200)
                self.assertEqual(len(claims["data"]["active"]), 1)
                self.assertEqual(len(claims["data"]["queue"]), 1)
            finally:
                server.close()


if __name__ == "__main__":
    unittest.main()
