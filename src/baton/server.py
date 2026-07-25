"""Dependency-free HTTP control plane for the BATON runtime."""

from __future__ import annotations

import hmac
import json
import mimetypes
import secrets
import sqlite3
import threading
import time
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from .config import ServiceConfig
from .core import RoutineSpec, Runtime


API_PREFIX = "/api/v1"


def _logical_time() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _row(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    for field in (
        "payload",
        "spec",
        "constraints",
        "advisories",
        "harness_state",
        "source_episodes",
        "gate_result",
    ):
        value = item.get(field)
        if isinstance(value, str) and value[:1] in {"{", "["}:
            try:
                item[field] = json.loads(value)
            except json.JSONDecodeError:
                pass
    if "retracted" in item:
        item["retracted"] = bool(item["retracted"])
    return item


def _require_text(payload: dict[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"{name} must be an array of non-empty strings")
    return tuple(item.strip() for item in value)


def _routine_spec(payload: dict[str, Any]) -> RoutineSpec:
    budget = payload.get("budget", {})
    coordination = payload.get("coordination", {})
    memory = payload.get("memory", {})
    if not isinstance(budget, dict):
        raise ValueError("budget must be an object")
    if not isinstance(coordination, dict):
        raise ValueError("coordination must be an object")
    if not isinstance(memory, dict):
        raise ValueError("memory must be an object")
    max_tokens = int(budget.get("max_tokens", payload.get("max_tokens", 10_000)))
    max_cost = float(budget.get("max_cost", payload.get("max_cost", 1.0)))
    max_wall = int(
        budget.get(
            "max_wall_seconds", payload.get("max_wall_seconds", 1_800)
        )
    )
    if max_tokens <= 0 or max_cost <= 0 or max_wall <= 0:
        raise ValueError("all budget limits must be positive")
    harness_state = payload.get("harness_state", {})
    if not isinstance(harness_state, dict):
        raise ValueError("harness_state must be an object")
    return RoutineSpec(
        name=_require_text(payload, "name"),
        agent=_require_text(payload, "agent"),
        schedule=_require_text(payload, "schedule"),
        objective=_require_text(payload, "objective"),
        constraints=_string_tuple(payload.get("constraints", []), "constraints"),
        max_tokens=max_tokens,
        max_cost=max_cost,
        max_wall_seconds=max_wall,
        claims=_string_tuple(coordination.get("claims", payload.get("claims")), "claims"),
        subscriptions=_string_tuple(
            coordination.get("subscribe", payload.get("subscriptions")),
            "subscriptions",
        ),
        memory_read=_string_tuple(
            memory.get("read", payload.get("memory_read", ["lessons"])),
            "memory.read",
        ),
        memory_write=_string_tuple(
            memory.get("write", payload.get("memory_write", ["episodes"])),
            "memory.write",
        ),
        workspace_ref=str(payload.get("workspace_ref", "git:workspace@main")),
        harness_state=harness_state,
    )


class BatonHTTPServer(ThreadingHTTPServer):
    """HTTP server carrying immutable service configuration."""

    daemon_threads = True

    def __init__(self, config: ServiceConfig):
        self.config = config.validate()
        self.write_lock = threading.Lock()
        super().__init__((self.config.host, self.config.port), BatonHandler)


class BatonHandler(BaseHTTPRequestHandler):
    """Small JSON API and optional same-origin static demo host."""

    server: BatonHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        print(
            json.dumps(
                {
                    "time": _logical_time(),
                    "client": self.client_address[0],
                    "message": format % args,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_OPTIONS(self) -> None:
        self._handle()

    def _handle(self) -> None:
        request_id = secrets.token_hex(8)
        self._request_id = request_id
        try:
            path = unquote(urlsplit(self.path).path)
            if path == "/healthz":
                self._json(
                    HTTPStatus.OK,
                    {"status": "ok", "service": "baton", "request_id": request_id},
                )
                return
            if path == "/readyz":
                with Runtime(self.server.config.database) as runtime:
                    runtime.connection.execute("SELECT 1").fetchone()
                self._json(
                    HTTPStatus.OK,
                    {
                        "status": "ready",
                        "database": "reachable",
                        "request_id": request_id,
                    },
                )
                return
            if path.startswith(API_PREFIX):
                self._authorize()
                if self.command == "OPTIONS":
                    self._options()
                    return
                if self.command == "POST":
                    with self.server.write_lock:
                        self._dispatch_api(path)
                else:
                    self._dispatch_api(path)
                return
            if self.command != "GET":
                raise APIError(HTTPStatus.METHOD_NOT_ALLOWED, "method_not_allowed")
            self._serve_static(path)
        except APIError as exc:
            self._error(exc.status, exc.code, exc.message)
        except json.JSONDecodeError:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_json", "invalid JSON body")
        except KeyError as exc:
            self._error(
                HTTPStatus.NOT_FOUND,
                "not_found",
                f"resource not found: {exc.args[0]}",
            )
        except ValueError as exc:
            self._error(HTTPStatus.UNPROCESSABLE_ENTITY, "invalid_request", str(exc))
        except sqlite3.IntegrityError as exc:
            self._error(HTTPStatus.CONFLICT, "conflict", str(exc))
        except RuntimeError as exc:
            self._error(HTTPStatus.CONFLICT, "invalid_state", str(exc))
        except Exception:
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "an internal error occurred",
            )

    def _authorize(self) -> None:
        config = self.server.config
        origin = self.headers.get("Origin")
        host_header = self.headers.get("Host", "")
        same_origins = {
            f"http://{host_header}",
            f"https://{host_header}",
        }
        if (
            origin
            and origin not in same_origins
            and origin not in config.allowed_origins
        ):
            raise APIError(
                HTTPStatus.FORBIDDEN,
                "origin_denied",
                "request origin is not allowed",
            )
        if config.loopback:
            host = (urlsplit(f"//{host_header}").hostname or "").lower()
            if host not in {"127.0.0.1", "::1", "localhost"}:
                raise APIError(
                    HTTPStatus.FORBIDDEN,
                    "host_denied",
                    "loopback service rejects non-loopback Host headers",
                )
        if config.api_token and self.command != "OPTIONS":
            authorization = self.headers.get("Authorization", "")
            supplied = (
                authorization[7:]
                if authorization.startswith("Bearer ")
                else self.headers.get("X-Baton-Token", "")
            )
            if not hmac.compare_digest(supplied, config.api_token):
                raise APIError(
                    HTTPStatus.UNAUTHORIZED,
                    "unauthorized",
                    "a valid BATON API token is required",
                )

    def _dispatch_api(self, path: str) -> None:
        parts = [item for item in path[len(API_PREFIX) :].split("/") if item]
        body = self._body() if self.command == "POST" else {}
        logical_time = str(body.get("logical_time") or _logical_time())
        now = int(body.get("now", time.time()))
        with Runtime(self.server.config.database) as runtime:
            if self.command == "GET" and parts == ["status"]:
                self._data(
                    {
                        "service": "baton",
                        "database": str(self.server.config.database),
                        "counts": {
                            table: len(runtime.rows(table))
                            for table in (
                                "routines",
                                "runs",
                                "claims",
                                "advisories",
                                "events",
                                "lessons",
                            )
                        },
                    }
                )
                return
            if self.command == "GET" and parts == ["routines"]:
                self._data([_row(item) for item in runtime.rows("routines")])
                return
            if self.command == "POST" and parts == ["routines"]:
                spec = _routine_spec(body)
                runtime.register_routine(spec, logical_time)
                self._data({"routine": spec.name}, HTTPStatus.CREATED)
                return
            if (
                self.command == "POST"
                and len(parts) == 3
                and parts[0] == "routines"
                and parts[2] == "fire"
            ):
                routine_name = parts[1]
                run_id = str(
                    body.get("run_id")
                    or f"{routine_name}-{int(time.time() * 1000)}"
                )
                runtime.fire_routine(routine_name, run_id, logical_time)
                routine = runtime.connection.execute(
                    "SELECT spec FROM routines WHERE name=?", (routine_name,)
                ).fetchone()
                raw = json.loads(routine["spec"])
                claim_results = []
                if body.get("acquire_claims", True):
                    for scope in raw.get("claims", []):
                        result = runtime.acquire_claim(
                            scope,
                            raw["agent"],
                            ttl_seconds=int(body.get("claim_ttl_seconds", 1800)),
                            now=now,
                            logical_time=logical_time,
                        )
                        claim_results.append({"scope": scope, **result})
                self._data(
                    {"run_id": run_id, "claims": claim_results},
                    HTTPStatus.CREATED,
                )
                return
            if self.command == "GET" and parts == ["runs"]:
                self._data([_row(item) for item in runtime.rows("runs")])
                return
            if self.command == "GET" and len(parts) == 2 and parts[0] == "runs":
                self._data(self._run_detail(runtime, parts[1]))
                return
            if (
                self.command == "GET"
                and len(parts) == 3
                and parts[0] == "runs"
                and parts[2] == "evidence"
            ):
                self._data(self._evidence(runtime, parts[1]))
                return
            if self.command == "POST" and len(parts) == 3 and parts[0] == "runs":
                self._run_action(
                    runtime, parts[1], parts[2], body, logical_time, now
                )
                return
            if self.command == "GET" and parts == ["claims"]:
                self._data(
                    {
                        "active": [_row(item) for item in runtime.rows("claims")],
                        "queue": [_row(item) for item in runtime.rows("claim_queue")],
                    }
                )
                return
            if self.command == "POST" and parts == ["claims"]:
                result = runtime.acquire_claim(
                    _require_text(body, "scope"),
                    _require_text(body, "owner"),
                    ttl_seconds=int(body.get("ttl_seconds", 1800)),
                    now=now,
                    logical_time=logical_time,
                )
                self._data(result, HTTPStatus.CREATED)
                return
            if (
                self.command == "POST"
                and len(parts) == 3
                and parts[0] == "claims"
            ):
                scope = parts[1]
                token = _require_text(body, "owner_token")
                if parts[2] == "renew":
                    renewed = runtime.renew_claim(
                        scope,
                        token,
                        ttl_seconds=int(body.get("ttl_seconds", 1800)),
                        now=now,
                        logical_time=logical_time,
                    )
                    self._data({"renewed": renewed})
                    return
                if parts[2] == "release":
                    self._data(
                        runtime.release_claim(
                            scope,
                            token,
                            now=now,
                            logical_time=logical_time,
                            next_ttl_seconds=int(
                                body.get("next_ttl_seconds", 1800)
                            ),
                        )
                    )
                    return
            if self.command == "GET" and parts == ["advisories"]:
                self._data([_row(item) for item in runtime.rows("advisories")])
                return
            if self.command == "POST" and parts == ["advisories"]:
                runtime.publish_advisory(
                    advisory_id=_require_text(body, "id"),
                    topic=_require_text(body, "topic"),
                    publisher=_require_text(body, "publisher"),
                    severity=_require_text(body, "severity"),
                    message=_require_text(body, "message"),
                    ttl_seconds=int(body.get("ttl_seconds", 3600)),
                    now=now,
                    logical_time=logical_time,
                )
                self._data({"advisory": body["id"]}, HTTPStatus.CREATED)
                return
            if (
                self.command == "POST"
                and len(parts) == 3
                and parts[0] == "advisories"
                and parts[2] == "retract"
            ):
                runtime.retract_advisory(
                    parts[1], _require_text(body, "publisher"), logical_time
                )
                self._data({"retracted": parts[1]})
                return
            if self.command == "GET" and parts == ["events"]:
                query = urlsplit(self.path).query
                params = dict(
                    item.split("=", 1) if "=" in item else (item, "")
                    for item in query.split("&")
                    if item
                )
                limit = min(max(int(params.get("limit", "500")), 1), 5000)
                run_id = params.get("run_id")
                if run_id:
                    rows = runtime.connection.execute(
                        """
                        SELECT * FROM events WHERE run_id=?
                        ORDER BY sequence DESC LIMIT ?
                        """,
                        (run_id, limit),
                    )
                else:
                    rows = runtime.connection.execute(
                        "SELECT * FROM events ORDER BY sequence DESC LIMIT ?",
                        (limit,),
                    )
                self._data([_row(item) for item in reversed(list(rows))])
                return
            if self.command == "GET" and parts == ["lessons"]:
                self._data([_row(item) for item in runtime.rows("lessons")])
                return
            if self.command == "GET" and parts == ["episodes"]:
                self._data([_row(item) for item in runtime.rows("episodes")])
                return
            if self.command == "POST" and parts == ["episodes"]:
                runtime.write_episode(
                    episode_id=_require_text(body, "id"),
                    run_id=_require_text(body, "run_id"),
                    task=_require_text(body, "task"),
                    trajectory_summary=_require_text(
                        body, "trajectory_summary"
                    ),
                    outcome=_require_text(body, "outcome"),
                    cost=float(body.get("cost", 0)),
                    logical_time=logical_time,
                )
                self._data({"episode": body["id"]}, HTTPStatus.CREATED)
                return
            if self.command == "POST" and parts == ["lessons", "gate"]:
                baseline = body.get("baseline_scores", [])
                candidate = body.get("candidate_scores", [])
                sources = body.get("source_episode_ids", [])
                if not isinstance(baseline, list) or not isinstance(candidate, list):
                    raise ValueError("score inputs must be arrays")
                if not isinstance(sources, list):
                    raise ValueError("source_episode_ids must be an array")
                self._data(
                    runtime.gate_lesson(
                        lesson_id=_require_text(body, "id"),
                        text=_require_text(body, "text"),
                        source_episode_ids=[str(item) for item in sources],
                        baseline_scores=[float(item) for item in baseline],
                        candidate_scores=[float(item) for item in candidate],
                        logical_time=logical_time,
                    ),
                    HTTPStatus.CREATED,
                )
                return
        raise APIError(HTTPStatus.NOT_FOUND, "not_found", "API route not found")

    def _run_action(
        self,
        runtime: Runtime,
        run_id: str,
        action: str,
        body: dict[str, Any],
        logical_time: str,
        now: int,
    ) -> None:
        runtime._run(run_id)
        if action == "resume":
            self._data(runtime.resume(run_id, logical_time))
            return
        if action == "objective":
            version = runtime.update_objective(
                run_id,
                issuer=_require_text(body, "issuer"),
                new_objective=_require_text(body, "objective"),
                step=int(body.get("step", runtime._run(run_id)["current_step"])),
                logical_time=logical_time,
            )
            self._data({"handoff_version": version})
            return
        if action == "checkpoint":
            state = body.get("harness_state", {})
            if not isinstance(state, dict):
                raise ValueError("harness_state must be an object")
            checkpoint_id = runtime.checkpoint(
                run_id,
                step=int(body["step"]),
                workspace_ref=_require_text(body, "workspace_ref"),
                harness_state=state,
                logical_time=logical_time,
            )
            self._data({"checkpoint_id": checkpoint_id}, HTTPStatus.CREATED)
            return
        if action == "tool-boundary":
            state = body.get("harness_state", {})
            result = body.get("result", {})
            if not isinstance(state, dict) or not isinstance(result, dict):
                raise ValueError("harness_state and result must be objects")
            runtime.tool_boundary(
                run_id,
                step=int(body["step"]),
                tool=_require_text(body, "tool"),
                result=result,
                workspace_ref=_require_text(body, "workspace_ref"),
                harness_state=state,
                logical_time=logical_time,
            )
            self._data({"checkpointed_step": int(body["step"])})
            return
        if action == "usage":
            signal = runtime.record_usage(
                run_id,
                tokens=int(body.get("tokens", 0)),
                cost=float(body.get("cost", 0)),
                wall_seconds=int(body.get("wall_seconds", 0)),
                logical_time=logical_time,
            )
            self._data({"signal": signal})
            return
        if action == "deliver-advisories":
            delivered = runtime.deliver_advisories(
                run_id,
                step=int(body["step"]),
                now=now,
                logical_time=logical_time,
            )
            self._data({"delivered": delivered})
            return
        if action == "advisory-reaction":
            runtime.record_advisory_reaction(
                run_id,
                _require_text(body, "advisory_id"),
                _require_text(body, "reaction"),
                logical_time,
            )
            self._data({"recorded": True})
            return
        if action == "complete":
            runtime.complete_run(
                run_id, _require_text(body, "outcome"), logical_time
            )
            self._data({"status": "COMPLETED"})
            return
        raise APIError(HTTPStatus.NOT_FOUND, "not_found", "run action not found")

    @staticmethod
    def _run_detail(runtime: Runtime, run_id: str) -> dict[str, Any]:
        run = _row(runtime._run(run_id))
        handoff = _row(runtime._handoff(run_id))
        checkpoint = runtime.connection.execute(
            "SELECT * FROM checkpoints WHERE run_id=? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        return {
            "run": run,
            "handoff": handoff,
            "checkpoint": _row(checkpoint) if checkpoint else None,
        }

    @staticmethod
    def _evidence(runtime: Runtime, run_id: str) -> dict[str, Any]:
        runtime._run(run_id)
        return {
            "run": BatonHandler._run_detail(runtime, run_id),
            "events": [
                _row(item)
                for item in runtime.connection.execute(
                    "SELECT * FROM events WHERE run_id=? ORDER BY sequence",
                    (run_id,),
                )
            ],
            "checkpoints": [
                _row(item)
                for item in runtime.connection.execute(
                    "SELECT * FROM checkpoints WHERE run_id=? ORDER BY id",
                    (run_id,),
                )
            ],
            "handoffs": [
                _row(item)
                for item in runtime.connection.execute(
                    "SELECT * FROM handoffs WHERE run_id=? ORDER BY version",
                    (run_id,),
                )
            ],
        }

    def _body(self) -> dict[str, Any]:
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            return {}
        length = int(content_length)
        if length < 0:
            raise APIError(
                HTTPStatus.BAD_REQUEST,
                "invalid_content_length",
                "Content-Length cannot be negative",
            )
        if length > self.server.config.max_body_bytes:
            raise APIError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "body_too_large",
                "request body exceeds configured maximum",
            )
        if length == 0:
            return {}
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0]
        if content_type != "application/json":
            raise APIError(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST bodies must use application/json",
            )
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _serve_static(self, path: str) -> None:
        static_dir = self.server.config.static_dir
        if static_dir is None:
            raise APIError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "static demo hosting is not enabled",
            )
        root = static_dir.resolve()
        relative = path.lstrip("/")
        target = (root / relative).resolve()
        if path.endswith("/") or target.is_dir():
            target = (target / "index.html").resolve()
        if root not in target.parents and target != root:
            raise APIError(HTTPStatus.FORBIDDEN, "forbidden", "invalid static path")
        if not target.is_file():
            raise APIError(HTTPStatus.NOT_FOUND, "not_found", "static file not found")
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        content = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; connect-src 'self'",
        )
        self.end_headers()
        self.wfile.write(content)

    def _options(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Authorization, Content-Type, X-Baton-Token",
        )
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _data(
        self, data: Any, status: HTTPStatus = HTTPStatus.OK
    ) -> None:
        self._json(
            status,
            {"data": data, "meta": {"request_id": self._request_id}},
        )

    def _error(self, status: HTTPStatus, code: str, message: str) -> None:
        self._json(
            status,
            {
                "error": {
                    "code": code,
                    "message": message,
                    "request_id": self._request_id,
                }
            },
        )

    def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        content = json.dumps(payload, sort_keys=True).encode()
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(content)

    def _cors(self) -> None:
        origin = self.headers.get("Origin")
        host_header = self.headers.get("Host", "")
        if origin and (
            origin in {f"http://{host_header}", f"https://{host_header}"}
            or origin in self.server.config.allowed_origins
        ):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")


class APIError(Exception):
    def __init__(
        self,
        status: HTTPStatus,
        code: str,
        message: str | None = None,
    ):
        self.status = status
        self.code = code
        self.message = message or code.replace("_", " ")
        super().__init__(self.message)


def create_server(config: ServiceConfig) -> BatonHTTPServer:
    """Create a configured server; useful for embedding and tests."""

    return BatonHTTPServer(config)


def serve(config: ServiceConfig) -> None:
    """Serve until interrupted."""

    server = create_server(config)
    host, port = server.server_address[:2]
    print(
        f"BATON control plane listening on http://{host}:{port} "
        f"(database={config.database})",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
