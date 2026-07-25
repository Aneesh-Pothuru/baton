"""Single-node durable runtime for structured agent coordination."""

from __future__ import annotations

import fnmatch
import hashlib
import html
import json
import math
import sqlite3
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RoutineSpec:
    name: str
    agent: str
    schedule: str
    objective: str
    constraints: tuple[str, ...]
    max_tokens: int
    max_cost: float
    max_wall_seconds: int
    claims: tuple[str, ...] = ()
    subscriptions: tuple[str, ...] = ()
    memory_read: tuple[str, ...] = ("lessons",)
    memory_write: tuple[str, ...] = ("episodes",)
    workspace_ref: str = "git:demo@main"
    harness_state: dict[str, Any] = field(default_factory=dict)


class Runtime:
    """SQLite-backed runtime whose durable state lives entirely in the DB."""

    def __init__(self, database: str | Path):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "Runtime":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                logical_time TEXT NOT NULL,
                kind TEXT NOT NULL,
                run_id TEXT,
                actor TEXT,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS routines (
                name TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                schedule TEXT NOT NULL,
                spec TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                routine TEXT,
                agent TEXT NOT NULL,
                objective TEXT NOT NULL,
                status TEXT NOT NULL,
                current_step INTEGER NOT NULL,
                current_handoff INTEGER NOT NULL,
                max_tokens INTEGER NOT NULL,
                max_cost REAL NOT NULL,
                max_wall_seconds INTEGER NOT NULL,
                used_tokens INTEGER NOT NULL DEFAULT 0,
                used_cost REAL NOT NULL DEFAULT 0,
                used_wall_seconds INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS handoffs (
                run_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                objective TEXT NOT NULL,
                constraints TEXT NOT NULL,
                advisories TEXT NOT NULL,
                issued_by TEXT NOT NULL,
                PRIMARY KEY(run_id, version),
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                step INTEGER NOT NULL,
                workspace_ref TEXT NOT NULL,
                harness_state TEXT NOT NULL,
                handoff_version INTEGER NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );
            CREATE TABLE IF NOT EXISTS claims (
                scope TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                owner_token TEXT NOT NULL UNIQUE,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS claim_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                owner TEXT NOT NULL,
                requested_at INTEGER NOT NULL,
                UNIQUE(scope, owner)
            );
            CREATE TABLE IF NOT EXISTS advisories (
                id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                publisher TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                retracted INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                run_id TEXT NOT NULL,
                pattern TEXT NOT NULL,
                PRIMARY KEY(run_id, pattern)
            );
            CREATE TABLE IF NOT EXISTS advisory_deliveries (
                advisory_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                delivered_step INTEGER NOT NULL,
                retracted_step INTEGER,
                PRIMARY KEY(advisory_id, run_id)
            );
            CREATE TABLE IF NOT EXISTS objective_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                issuer TEXT NOT NULL,
                previous_objective TEXT NOT NULL,
                new_objective TEXT NOT NULL,
                applied_step INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS episodes (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                task TEXT NOT NULL,
                trajectory_summary TEXT NOT NULL,
                outcome TEXT NOT NULL,
                cost REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lessons (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                status TEXT NOT NULL,
                source_episodes TEXT NOT NULL,
                gate_result TEXT NOT NULL,
                mean_delta REAL,
                win_rate REAL
            );
            """
        )
        self.connection.commit()

    def event(
        self,
        logical_time: str,
        kind: str,
        *,
        run_id: str | None = None,
        actor: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO events(logical_time,kind,run_id,actor,payload)
            VALUES(?,?,?,?,?)
            """,
            (
                logical_time,
                kind,
                run_id,
                actor,
                json.dumps(payload or {}, sort_keys=True),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def register_routine(self, spec: RoutineSpec, logical_time: str) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO routines(name,agent,schedule,spec) VALUES(?,?,?,?)",
            (spec.name, spec.agent, spec.schedule, json.dumps(asdict(spec), sort_keys=True)),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "ROUTINE_REGISTERED",
            actor=spec.agent,
            payload={"name": spec.name, "schedule": spec.schedule},
        )

    def fire_routine(
        self, name: str, run_id: str, logical_time: str
    ) -> str:
        row = self.connection.execute(
            "SELECT spec FROM routines WHERE name=?", (name,)
        ).fetchone()
        if row is None:
            raise KeyError(f"routine not found: {name}")
        raw = json.loads(row["spec"])
        spec = RoutineSpec(
            name=raw["name"],
            agent=raw["agent"],
            schedule=raw["schedule"],
            objective=raw["objective"],
            constraints=tuple(raw["constraints"]),
            max_tokens=int(raw["max_tokens"]),
            max_cost=float(raw["max_cost"]),
            max_wall_seconds=int(raw["max_wall_seconds"]),
            claims=tuple(raw["claims"]),
            subscriptions=tuple(raw["subscriptions"]),
            memory_read=tuple(raw["memory_read"]),
            memory_write=tuple(raw["memory_write"]),
            workspace_ref=raw["workspace_ref"],
            harness_state=raw["harness_state"],
        )
        self.start_run(
            run_id=run_id,
            agent=spec.agent,
            objective=spec.objective,
            constraints=spec.constraints,
            max_tokens=spec.max_tokens,
            max_cost=spec.max_cost,
            max_wall_seconds=spec.max_wall_seconds,
            workspace_ref=spec.workspace_ref,
            harness_state=spec.harness_state,
            logical_time=logical_time,
            routine=name,
        )
        for pattern in spec.subscriptions:
            self.subscribe(run_id, pattern)
        self.event(
            logical_time,
            "ROUTINE_FIRED",
            run_id=run_id,
            actor=spec.agent,
            payload={"routine": name, "schedule": spec.schedule},
        )
        return run_id

    def start_run(
        self,
        *,
        run_id: str,
        agent: str,
        objective: str,
        constraints: tuple[str, ...],
        max_tokens: int,
        max_cost: float,
        max_wall_seconds: int,
        workspace_ref: str,
        harness_state: dict[str, Any],
        logical_time: str,
        routine: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO runs(
              id,routine,agent,objective,status,current_step,current_handoff,
              max_tokens,max_cost,max_wall_seconds
            ) VALUES(?,?,?,?,?,0,1,?,?,?)
            """,
            (
                run_id,
                routine,
                agent,
                objective,
                "RUNNING",
                max_tokens,
                max_cost,
                max_wall_seconds,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO handoffs(
              run_id,version,objective,constraints,advisories,issued_by
            ) VALUES(?,1,?,?,?,'routine')
            """,
            (run_id, objective, json.dumps(constraints), "[]"),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "RUN_STARTED",
            run_id=run_id,
            actor=agent,
            payload={"objective": objective, "routine": routine},
        )
        self.checkpoint(
            run_id,
            step=0,
            workspace_ref=workspace_ref,
            harness_state=harness_state,
            logical_time=logical_time,
        )

    def _run(self, run_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return row

    def _handoff(self, run_id: str, version: int | None = None) -> sqlite3.Row:
        if version is None:
            version = int(self._run(run_id)["current_handoff"])
        row = self.connection.execute(
            "SELECT * FROM handoffs WHERE run_id=? AND version=?",
            (run_id, version),
        ).fetchone()
        if row is None:
            raise KeyError(f"handoff not found: {run_id} v{version}")
        return row

    def _write_handoff(
        self,
        run_id: str,
        *,
        objective: str,
        constraints: list[str],
        advisories: list[dict[str, Any]],
        issued_by: str,
    ) -> int:
        current = int(self._run(run_id)["current_handoff"])
        version = current + 1
        self.connection.execute(
            """
            INSERT INTO handoffs(
              run_id,version,objective,constraints,advisories,issued_by
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                run_id,
                version,
                objective,
                json.dumps(constraints, sort_keys=True),
                json.dumps(advisories, sort_keys=True),
                issued_by,
            ),
        )
        self.connection.execute(
            "UPDATE runs SET current_handoff=?,objective=? WHERE id=?",
            (version, objective, run_id),
        )
        self.connection.commit()
        return version

    def checkpoint(
        self,
        run_id: str,
        *,
        step: int,
        workspace_ref: str,
        harness_state: dict[str, Any],
        logical_time: str,
    ) -> int:
        handoff_version = int(self._run(run_id)["current_handoff"])
        cursor = self.connection.execute(
            """
            INSERT INTO checkpoints(
              run_id,step,workspace_ref,harness_state,handoff_version
            ) VALUES(?,?,?,?,?)
            """,
            (
                run_id,
                step,
                workspace_ref,
                json.dumps(harness_state, sort_keys=True),
                handoff_version,
            ),
        )
        self.connection.execute(
            "UPDATE runs SET current_step=? WHERE id=?", (step, run_id)
        )
        self.connection.commit()
        self.event(
            logical_time,
            "CHECKPOINT_WRITTEN",
            run_id=run_id,
            payload={
                "checkpoint_id": cursor.lastrowid,
                "step": step,
                "workspace_ref": workspace_ref,
                "handoff_version": handoff_version,
            },
        )
        return int(cursor.lastrowid)

    def tool_boundary(
        self,
        run_id: str,
        *,
        step: int,
        tool: str,
        result: dict[str, Any],
        workspace_ref: str,
        harness_state: dict[str, Any],
        logical_time: str,
    ) -> None:
        self.event(
            logical_time,
            "TOOL_CALL_COMPLETED",
            run_id=run_id,
            actor=str(self._run(run_id)["agent"]),
            payload={"step": step, "tool": tool, "result": result},
        )
        self.checkpoint(
            run_id,
            step=step,
            workspace_ref=workspace_ref,
            harness_state=harness_state,
            logical_time=logical_time,
        )

    def resume(self, run_id: str, logical_time: str) -> dict[str, Any]:
        checkpoint = self.connection.execute(
            "SELECT * FROM checkpoints WHERE run_id=? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if checkpoint is None:
            raise RuntimeError(f"run has no checkpoint: {run_id}")
        handoff = self._handoff(run_id, int(checkpoint["handoff_version"]))
        self.connection.execute(
            "UPDATE runs SET status='RUNNING',current_step=?,current_handoff=? WHERE id=?",
            (checkpoint["step"], checkpoint["handoff_version"], run_id),
        )
        self.connection.commit()
        restored = {
            "run_id": run_id,
            "step": int(checkpoint["step"]),
            "workspace_ref": str(checkpoint["workspace_ref"]),
            "harness_state": json.loads(checkpoint["harness_state"]),
            "handoff": {
                "version": int(handoff["version"]),
                "objective": str(handoff["objective"]),
                "constraints": json.loads(handoff["constraints"]),
                "advisories": json.loads(handoff["advisories"]),
            },
        }
        self.event(
            logical_time,
            "RUN_RESUMED",
            run_id=run_id,
            actor=str(self._run(run_id)["agent"]),
            payload={
                "step": restored["step"],
                "workspace_ref": restored["workspace_ref"],
                "handoff_version": restored["handoff"]["version"],
            },
        )
        return restored

    def record_usage(
        self,
        run_id: str,
        *,
        tokens: int,
        cost: float,
        wall_seconds: int,
        logical_time: str,
    ) -> str:
        if tokens < 0 or cost < 0 or wall_seconds < 0:
            raise ValueError("usage deltas cannot be negative")
        run = self._run(run_id)
        used_tokens = int(run["used_tokens"]) + tokens
        used_cost = float(run["used_cost"]) + cost
        used_wall = int(run["used_wall_seconds"]) + wall_seconds
        ratios = [
            used_tokens / int(run["max_tokens"]),
            used_cost / float(run["max_cost"]),
            used_wall / int(run["max_wall_seconds"]),
        ]
        ratio = max(ratios)
        if ratio > 1:
            signal = "STOP"
            status = "PAUSED_BUDGET"
        elif ratio >= 0.8:
            signal = "RESCOPE"
            status = str(run["status"])
        else:
            signal = "CONTINUE"
            status = str(run["status"])
        self.connection.execute(
            """
            UPDATE runs SET used_tokens=?,used_cost=?,used_wall_seconds=?,status=?
            WHERE id=?
            """,
            (used_tokens, used_cost, used_wall, status, run_id),
        )
        self.connection.commit()
        self.event(
            logical_time,
            f"BUDGET_{signal}",
            run_id=run_id,
            payload={
                "tokens": used_tokens,
                "cost": used_cost,
                "wall_seconds": used_wall,
                "max_ratio": ratio,
            },
        )
        return signal

    @staticmethod
    def _claim_token(scope: str, owner: str, now: int) -> str:
        return hashlib.sha256(f"{scope}|{owner}|{now}".encode()).hexdigest()[:16]

    def acquire_claim(
        self,
        scope: str,
        owner: str,
        *,
        ttl_seconds: int,
        now: int,
        logical_time: str,
    ) -> dict[str, Any]:
        if ttl_seconds <= 0:
            raise ValueError("claim TTL must be positive")
        active = self.connection.execute(
            "SELECT * FROM claims WHERE scope=?", (scope,)
        ).fetchone()
        if active is not None and int(active["expires_at"]) <= now:
            self.connection.execute("DELETE FROM claims WHERE scope=?", (scope,))
            self.connection.commit()
            self.event(
                logical_time,
                "CLAIM_EXPIRED",
                actor=str(active["owner"]),
                payload={"scope": scope},
            )
            queued = self.connection.execute(
                "SELECT * FROM claim_queue WHERE scope=? ORDER BY id LIMIT 1",
                (scope,),
            ).fetchone()
            if queued is None:
                active = None
            else:
                self.connection.execute(
                    "DELETE FROM claim_queue WHERE id=?", (queued["id"],)
                )
                next_owner = str(queued["owner"])
                token = self._claim_token(scope, next_owner, now)
                self.connection.execute(
                    """
                    INSERT INTO claims(scope,owner,owner_token,expires_at)
                    VALUES(?,?,?,?)
                    """,
                    (scope, next_owner, token, now + ttl_seconds),
                )
                self.connection.commit()
                self.event(
                    logical_time,
                    "CLAIM_GRANTED",
                    actor=next_owner,
                    payload={
                        "scope": scope,
                        "owner_token": token,
                        "expires_at": now + ttl_seconds,
                        "from_expiry_queue": True,
                    },
                )
                active = self.connection.execute(
                    "SELECT * FROM claims WHERE scope=?", (scope,)
                ).fetchone()
        if active is None:
            token = self._claim_token(scope, owner, now)
            self.connection.execute(
                "INSERT INTO claims(scope,owner,owner_token,expires_at) VALUES(?,?,?,?)",
                (scope, owner, token, now + ttl_seconds),
            )
            self.connection.commit()
            self.event(
                logical_time,
                "CLAIM_GRANTED",
                actor=owner,
                payload={
                    "scope": scope,
                    "owner_token": token,
                    "expires_at": now + ttl_seconds,
                },
            )
            return {"status": "GRANTED", "owner_token": token}
        if active["owner"] == owner:
            return {
                "status": "GRANTED",
                "owner_token": str(active["owner_token"]),
            }
        self.connection.execute(
            """
            INSERT OR IGNORE INTO claim_queue(scope,owner,requested_at)
            VALUES(?,?,?)
            """,
            (scope, owner, now),
        )
        self.connection.commit()
        position = int(
            self.connection.execute(
                "SELECT COUNT(*) AS n FROM claim_queue WHERE scope=? AND id <= "
                "(SELECT id FROM claim_queue WHERE scope=? AND owner=?)",
                (scope, scope, owner),
            ).fetchone()["n"]
        )
        self.event(
            logical_time,
            "CLAIM_QUEUED",
            actor=owner,
            payload={"scope": scope, "position": position, "active_owner": active["owner"]},
        )
        return {"status": "QUEUED", "position": position}

    def renew_claim(
        self,
        scope: str,
        owner_token: str,
        *,
        ttl_seconds: int,
        now: int,
        logical_time: str,
    ) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("claim TTL must be positive")
        cursor = self.connection.execute(
            """
            UPDATE claims SET expires_at=?
            WHERE scope=? AND owner_token=? AND expires_at>?
            """,
            (now + ttl_seconds, scope, owner_token, now),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "CLAIM_RENEWED" if cursor.rowcount == 1 else "CLAIM_RENEW_REJECTED",
            payload={"scope": scope, "expires_at": now + ttl_seconds},
        )
        return cursor.rowcount == 1

    def release_claim(
        self,
        scope: str,
        owner_token: str,
        *,
        now: int,
        logical_time: str,
        next_ttl_seconds: int = 1800,
    ) -> dict[str, Any]:
        if next_ttl_seconds <= 0:
            raise ValueError("next claim TTL must be positive")
        active = self.connection.execute(
            "SELECT * FROM claims WHERE scope=?", (scope,)
        ).fetchone()
        if active is None or active["owner_token"] != owner_token:
            self.event(
                logical_time,
                "CLAIM_RELEASE_REJECTED",
                actor=str(active["owner"]) if active else None,
                payload={"scope": scope, "reason": "owner-token-mismatch"},
            )
            return {"released": False, "granted_next": None}
        owner = str(active["owner"])
        self.connection.execute(
            "DELETE FROM claims WHERE scope=? AND owner_token=?",
            (scope, owner_token),
        )
        queued = self.connection.execute(
            "SELECT * FROM claim_queue WHERE scope=? ORDER BY id LIMIT 1",
            (scope,),
        ).fetchone()
        granted_next: dict[str, Any] | None = None
        if queued is not None:
            self.connection.execute(
                "DELETE FROM claim_queue WHERE id=?", (queued["id"],)
            )
            next_owner = str(queued["owner"])
            token = self._claim_token(scope, next_owner, now)
            self.connection.execute(
                "INSERT INTO claims(scope,owner,owner_token,expires_at) VALUES(?,?,?,?)",
                (scope, next_owner, token, now + next_ttl_seconds),
            )
            granted_next = {"owner": next_owner, "owner_token": token}
        self.connection.commit()
        self.event(
            logical_time,
            "CLAIM_RELEASED",
            actor=owner,
            payload={"scope": scope, "granted_next": granted_next},
        )
        if granted_next:
            self.event(
                logical_time,
                "CLAIM_GRANTED",
                actor=granted_next["owner"],
                payload={
                    "scope": scope,
                    "owner_token": granted_next["owner_token"],
                    "from_queue": True,
                },
            )
        return {"released": True, "granted_next": granted_next}

    def subscribe(self, run_id: str, pattern: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO subscriptions(run_id,pattern) VALUES(?,?)",
            (run_id, pattern),
        )
        self.connection.commit()

    def publish_advisory(
        self,
        *,
        advisory_id: str,
        topic: str,
        publisher: str,
        severity: str,
        message: str,
        ttl_seconds: int,
        now: int,
        logical_time: str,
    ) -> None:
        if severity not in {"info", "warn", "critical"}:
            raise ValueError("invalid advisory severity")
        if ttl_seconds <= 0:
            raise ValueError("advisory TTL must be positive")
        self.connection.execute(
            """
            INSERT INTO advisories(
              id,topic,publisher,severity,message,created_at,expires_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                advisory_id,
                topic,
                publisher,
                severity,
                message,
                now,
                now + ttl_seconds,
            ),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "ADVISORY_PUBLISHED",
            actor=publisher,
            payload={
                "id": advisory_id,
                "topic": topic,
                "severity": severity,
                "ttl_seconds": ttl_seconds,
                "message": message,
            },
        )

    def retract_advisory(
        self, advisory_id: str, publisher: str, logical_time: str
    ) -> None:
        cursor = self.connection.execute(
            "UPDATE advisories SET retracted=1 WHERE id=? AND publisher=?",
            (advisory_id, publisher),
        )
        if cursor.rowcount != 1:
            raise KeyError(advisory_id)
        self.connection.commit()
        self.event(
            logical_time,
            "ADVISORY_RETRACTED",
            actor=publisher,
            payload={"id": advisory_id},
        )

    def deliver_advisories(
        self, run_id: str, *, step: int, now: int, logical_time: str
    ) -> list[dict[str, Any]]:
        patterns = [
            str(row["pattern"])
            for row in self.connection.execute(
                "SELECT pattern FROM subscriptions WHERE run_id=?", (run_id,)
            )
        ]
        handoff = self._handoff(run_id)
        pinned: list[dict[str, Any]] = json.loads(handoff["advisories"])
        changed = False
        delivered: list[dict[str, Any]] = []

        active_by_id = {str(item["id"]): item for item in pinned}
        for item in list(pinned):
            advisory = self.connection.execute(
                "SELECT * FROM advisories WHERE id=?", (item["id"],)
            ).fetchone()
            if advisory is None or advisory["retracted"] or advisory["expires_at"] <= now:
                pinned.remove(item)
                changed = True
                delivery = self.connection.execute(
                    """
                    SELECT * FROM advisory_deliveries
                    WHERE advisory_id=? AND run_id=?
                    """,
                    (item["id"], run_id),
                ).fetchone()
                if delivery and delivery["retracted_step"] is None:
                    self.connection.execute(
                        """
                        UPDATE advisory_deliveries SET retracted_step=?
                        WHERE advisory_id=? AND run_id=?
                        """,
                        (step, item["id"], run_id),
                    )
                self.event(
                    logical_time,
                    "ADVISORY_REMOVED_AT_BOUNDARY",
                    run_id=run_id,
                    payload={"id": item["id"], "step": step},
                )

        advisories = self.connection.execute(
            """
            SELECT * FROM advisories
            WHERE retracted=0 AND expires_at>? ORDER BY created_at,id
            """,
            (now,),
        )
        for advisory in advisories:
            topic = str(advisory["topic"])
            if not any(fnmatch.fnmatchcase(topic, pattern) for pattern in patterns):
                continue
            advisory_id = str(advisory["id"])
            if advisory_id in active_by_id:
                continue
            existing = self.connection.execute(
                """
                SELECT 1 FROM advisory_deliveries
                WHERE advisory_id=? AND run_id=?
                """,
                (advisory_id, run_id),
            ).fetchone()
            if existing:
                continue
            payload = {
                "id": advisory_id,
                "topic": topic,
                "publisher": str(advisory["publisher"]),
                "severity": str(advisory["severity"]),
                "message": str(advisory["message"]),
                "expires_at": int(advisory["expires_at"]),
                "provenance": "baton.advisory",
                "treated_as": "data",
            }
            pinned.append(payload)
            delivered.append(payload)
            changed = True
            self.connection.execute(
                """
                INSERT INTO advisory_deliveries(
                  advisory_id,run_id,delivered_step
                ) VALUES(?,?,?)
                """,
                (advisory_id, run_id, step),
            )
            self.event(
                logical_time,
                "ADVISORY_DELIVERED",
                run_id=run_id,
                actor=str(advisory["publisher"]),
                payload={"id": advisory_id, "topic": topic, "step": step},
            )

        if changed:
            self._write_handoff(
                run_id,
                objective=str(handoff["objective"]),
                constraints=json.loads(handoff["constraints"]),
                advisories=pinned,
                issued_by="advisory-bus",
            )
        self.connection.commit()
        return delivered

    def record_advisory_reaction(
        self,
        run_id: str,
        advisory_id: str,
        reaction: str,
        logical_time: str,
    ) -> None:
        delivered = self.connection.execute(
            """
            SELECT 1 FROM advisory_deliveries
            WHERE advisory_id=? AND run_id=?
            """,
            (advisory_id, run_id),
        ).fetchone()
        if delivered is None:
            raise RuntimeError("cannot react to an undelivered advisory")
        self.event(
            logical_time,
            "ADVISORY_REACTION",
            run_id=run_id,
            actor=str(self._run(run_id)["agent"]),
            payload={"advisory_id": advisory_id, "reaction": reaction},
        )

    def update_objective(
        self,
        run_id: str,
        *,
        issuer: str,
        new_objective: str,
        step: int,
        logical_time: str,
    ) -> int:
        if not issuer.strip():
            raise ValueError("objective update needs an explicit issuer")
        current = self._handoff(run_id)
        previous = str(current["objective"])
        version = self._write_handoff(
            run_id,
            objective=new_objective,
            constraints=json.loads(current["constraints"]),
            advisories=json.loads(current["advisories"]),
            issued_by=issuer,
        )
        self.connection.execute(
            """
            INSERT INTO objective_updates(
              run_id,issuer,previous_objective,new_objective,applied_step
            ) VALUES(?,?,?,?,?)
            """,
            (run_id, issuer, previous, new_objective, step),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "OBJECTIVE_UPDATED",
            run_id=run_id,
            actor=issuer,
            payload={
                "previous": previous,
                "new": new_objective,
                "step": step,
                "handoff_version": version,
            },
        )
        return version

    def write_episode(
        self,
        *,
        episode_id: str,
        run_id: str,
        task: str,
        trajectory_summary: str,
        outcome: str,
        cost: float,
        logical_time: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO episodes(
              id,run_id,task,trajectory_summary,outcome,cost
            ) VALUES(?,?,?,?,?,?)
            """,
            (episode_id, run_id, task, trajectory_summary, outcome, cost),
        )
        self.connection.commit()
        self.event(
            logical_time,
            "EPISODE_WRITTEN",
            run_id=run_id,
            payload={"episode_id": episode_id, "outcome": outcome, "cost": cost},
        )

    def gate_lesson(
        self,
        *,
        lesson_id: str,
        text: str,
        source_episode_ids: list[str],
        baseline_scores: list[float],
        candidate_scores: list[float],
        logical_time: str,
    ) -> dict[str, Any]:
        known_sources: set[str] = set()
        if source_episode_ids:
            placeholders = ",".join("?" for _ in source_episode_ids)
            known_sources = {
                str(row["id"])
                for row in self.connection.execute(
                    f"SELECT id FROM episodes WHERE id IN ({placeholders})",
                    source_episode_ids,
                )
            }
        valid = (
            bool(baseline_scores)
            and len(baseline_scores) == len(candidate_scores)
            and all(math.isfinite(value) for value in baseline_scores)
            and all(math.isfinite(value) for value in candidate_scores)
            and bool(source_episode_ids)
            and len(set(source_episode_ids)) == len(source_episode_ids)
            and known_sources == set(source_episode_ids)
        )
        if not valid:
            status = "UNDETERMINED"
            mean_delta = None
            win_rate = None
            result = "UNDETERMINED"
        else:
            deltas = [
                candidate - baseline
                for baseline, candidate in zip(
                    baseline_scores, candidate_scores, strict=True
                )
            ]
            mean_delta = statistics.fmean(deltas)
            win_rate = sum(delta > 0 for delta in deltas) / len(deltas)
            if mean_delta > 0:
                status = "PROMOTED"
                result = "PASS"
            else:
                status = "ARCHIVED"
                result = "FAIL"
        self.connection.execute(
            """
            INSERT INTO lessons(
              id,text,status,source_episodes,gate_result,mean_delta,win_rate
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                lesson_id,
                text,
                status,
                json.dumps(source_episode_ids),
                result,
                mean_delta,
                win_rate,
            ),
        )
        self.connection.commit()
        payload = {
            "lesson_id": lesson_id,
            "status": status,
            "gate_result": result,
            "source_episodes": source_episode_ids,
            "mean_delta": mean_delta,
            "win_rate": win_rate,
        }
        self.event(
            logical_time,
            "LESSON_GATE_EVALUATED",
            actor="memory-distiller",
            payload=payload,
        )
        return payload

    def complete_run(
        self, run_id: str, outcome: str, logical_time: str
    ) -> None:
        self.connection.execute(
            "UPDATE runs SET status='COMPLETED' WHERE id=?", (run_id,)
        )
        self.connection.commit()
        self.event(
            logical_time,
            "RUN_COMPLETED",
            run_id=run_id,
            actor=str(self._run(run_id)["agent"]),
            payload={"outcome": outcome},
        )

    def rows(self, table: str) -> list[sqlite3.Row]:
        allowed = {
            "events",
            "routines",
            "runs",
            "handoffs",
            "checkpoints",
            "claims",
            "claim_queue",
            "advisories",
            "subscriptions",
            "advisory_deliveries",
            "objective_updates",
            "episodes",
            "lessons",
        }
        if table not in allowed:
            raise ValueError(table)
        return list(self.connection.execute(f"SELECT * FROM {table}"))  # noqa: S608


def render_timeline(runtime: Runtime, output: str | Path) -> Path:
    """Render the durable office record as an inspectable conductor's score."""

    events = runtime.rows("events")
    runs = runtime.rows("runs")
    lessons = runtime.rows("lessons")
    promoted = sum(row["status"] == "PROMOTED" for row in lessons)
    undetermined = sum(row["status"] == "UNDETERMINED" for row in lessons)
    claims = sum(row["kind"] == "CLAIM_GRANTED" for row in events)
    advisories = sum(row["kind"] == "ADVISORY_PUBLISHED" for row in events)

    def family(kind: str) -> str:
        if "ADVISORY" in kind or "OBJECTIVE" in kind:
            return "direction"
        if "CLAIM" in kind:
            return "coordination"
        if "LESSON" in kind or "EPISODE" in kind:
            return "memory"
        if "RUN_" in kind or "ROUTINE_" in kind:
            return "run"
        if "CHECKPOINT" in kind or "TOOL_" in kind:
            return "execution"
        return "system"

    actor_names = ["control-plane"]
    for event in events:
        actor = str(event["actor"] or "control-plane")
        if actor not in actor_names:
            actor_names.append(actor)

    event_count = max(len(events), 1)
    event_nodes: dict[str, list[str]] = {actor: [] for actor in actor_names}
    for row in events:
        sequence = int(row["sequence"])
        logical_time = str(row["logical_time"])
        actor = str(row["actor"] or "control-plane")
        kind = str(row["kind"])
        run_id = str(row["run_id"] or "fleet")
        payload = str(row["payload"])
        marker = (
            f"<button class='score-note {family(kind)}' "
            f"style='grid-column:{sequence}' data-kind='{html.escape(kind)}' "
            f"data-sequence='{sequence}' "
            f"data-time='{html.escape(logical_time, quote=True)}' "
            f"data-actor='{html.escape(actor, quote=True)}' "
            f"data-run='{html.escape(run_id, quote=True)}' "
            f"data-payload='{html.escape(payload, quote=True)}' "
            f"aria-label='Event {sequence}: {html.escape(kind)} by "
            f"{html.escape(actor)} at {html.escape(logical_time)}'>"
            f"<span>{sequence}</span><b class='sr-only'>{html.escape(kind)}</b>"
            "</button>"
        )
        event_nodes[actor].append(marker)

    score_lanes = "\n".join(
        "<div class='score-row'>"
        f"<div class='lane-label'><strong>{html.escape(actor)}</strong>"
        f"<span>{len(event_nodes[actor])} cues</span></div>"
        f"<div class='staff' style='--beats:{event_count}' "
        f"aria-label='{html.escape(actor)} event lane'>"
        f"{''.join(event_nodes[actor])}</div></div>"
        for actor in actor_names
    )
    time_cells = "\n".join(
        f"<span>{html.escape(str(row['logical_time'])[11:16])}</span>"
        for row in events
    )
    performer_rows = "\n".join(
        "<li>"
        f"<span class='part'>{index:02d}</span><div><strong>"
        f"{html.escape(str(row['agent']))}</strong>"
        f"<small>{html.escape(str(row['id']))}</small></div>"
        f"<span class='performer-status'>{html.escape(str(row['status']))}</span>"
        f"<div class='budget' title='{int(row['used_tokens'])} of "
        f"{int(row['max_tokens'])} tokens used'><i style='width:"
        f"{min(100, (int(row['used_tokens']) / int(row['max_tokens'])) * 100):.0f}%'>"
        "</i></div></li>"
        for index, row in enumerate(runs, 1)
    )
    transcript_rows = "\n".join(
        f"<tr data-kind='{html.escape(str(row['kind']))}'>"
        f"<td>{row['sequence']}</td>"
        f"<td>{html.escape(str(row['logical_time']))}</td>"
        f"<td>{html.escape(str(row['actor'] or 'control-plane'))}</td>"
        f"<td>{html.escape(str(row['kind']))}</td>"
        f"<td>{html.escape(str(row['run_id'] or 'fleet'))}</td>"
        f"<td><code>{html.escape(str(row['payload']))}</code></td></tr>"
        for row in events
    )
    seed_events_json = json.dumps(
        [
            {
                "sequence": int(row["sequence"]),
                "time": str(row["logical_time"]),
                "actor": str(row["actor"] or "control-plane"),
                "kind": str(row["kind"]),
                "run": str(row["run_id"] or "fleet"),
                "payload": json.loads(str(row["payload"])),
                "family": family(str(row["kind"])),
            }
            for row in events
        ],
        separators=(",", ":"),
    ).replace("</", "<\\/")
    seed_runs_json = json.dumps(
        [
            {
                "id": str(row["id"]),
                "agent": str(row["agent"]),
                "status": str(row["status"]),
                "usedTokens": int(row["used_tokens"]),
                "maxTokens": int(row["max_tokens"]),
            }
            for row in runs
        ],
        separators=(",", ":"),
    ).replace("</", "<\\/")
    stylesheet = """
:root {
  --paper:#f2f0e8;
  --sheet:#fffef8;
  --ink:#171714;
  --muted:#67665f;
  --hair:#c9c5b7;
  --blue:#2343e8;
  --red:#ed5b3d;
  --acid:#c8ff32;
  --mint:#5ed5ab;
  --shadow:0 22px 60px rgba(34,31,22,.12);
}
* { box-sizing:border-box; }
html { scroll-behavior:smooth; }
body {
  margin:0;
  color:var(--ink);
  background:
    linear-gradient(rgba(23,23,20,.035) 1px,transparent 1px),
    linear-gradient(90deg,rgba(23,23,20,.035) 1px,transparent 1px),
    var(--paper);
  background-size:28px 28px;
  font:15px/1.5 Arial,Helvetica,sans-serif;
}
button,select { font:inherit; }
button:focus-visible,select:focus-visible,summary:focus-visible {
  outline:3px solid var(--blue);
  outline-offset:3px;
}
.skip-link {
  position:absolute;
  left:14px;
  top:-70px;
  z-index:100;
  padding:10px 14px;
  color:var(--ink);
  background:var(--acid);
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-decoration:none;
  text-transform:uppercase;
}
.skip-link:focus { top:12px; }
.sr-only {
  position:absolute;
  width:1px;
  height:1px;
  padding:0;
  margin:-1px;
  overflow:hidden;
  clip:rect(0,0,0,0);
  white-space:nowrap;
  border:0;
}
.masthead {
  min-height:54px;
  padding:0 3.2vw;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:24px;
  color:#fff;
  background:var(--ink);
  border-bottom:5px solid var(--acid);
}
.wordmark {
  display:flex;
  align-items:baseline;
  gap:12px;
  font-weight:900;
  letter-spacing:-.04em;
}
.wordmark span {
  color:var(--acid);
  font:600 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.13em;
  text-transform:uppercase;
}
.mast-meta {
  display:flex;
  gap:24px;
  color:#c9c9c2;
  font:600 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.1em;
  text-transform:uppercase;
}
.mast-meta strong { color:var(--acid); }
.score-nav {
  display:flex;
  align-items:center;
  gap:18px;
}
.score-nav a {
  color:#c9c9c2;
  text-decoration:none;
  font:700 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.08em;
  text-transform:uppercase;
}
.score-nav a:hover { color:var(--acid); }
main { overflow:hidden; }
.opening {
  max-width:1500px;
  margin:auto;
  padding:62px 3.2vw 42px;
  display:grid;
  grid-template-columns:minmax(0,1.45fr) minmax(330px,.55fr);
  gap:6vw;
  align-items:end;
}
.kicker,.edition {
  margin:0 0 14px;
  font:700 11px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.14em;
  text-transform:uppercase;
}
.kicker { color:var(--blue); }
h1 {
  max-width:900px;
  margin:0;
  font:400 clamp(54px,8.4vw,130px)/.78 Georgia,"Times New Roman",serif;
  letter-spacing:-.075em;
}
h1 em {
  position:relative;
  z-index:0;
  font-style:italic;
}
h1 em:after {
  content:"";
  position:absolute;
  z-index:-1;
  left:-.04em;
  right:-.08em;
  bottom:.06em;
  height:.24em;
  background:var(--acid);
  transform:rotate(-1.2deg);
}
.opening-note {
  padding:24px 0 4px 25px;
  border-left:2px solid var(--ink);
}
.opening-note p {
  max-width:420px;
  margin:0 0 24px;
  font:18px/1.45 Georgia,"Times New Roman",serif;
}
.principles {
  display:grid;
  grid-template-columns:1fr 1fr;
  border-top:1px solid var(--ink);
  border-bottom:1px solid var(--ink);
}
.principles span {
  padding:9px 0;
  font:700 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.principles span:nth-child(even) { text-align:right; }
.measure-strip {
  display:grid;
  grid-template-columns:repeat(4,1fr);
  border-top:2px solid var(--ink);
  border-bottom:2px solid var(--ink);
  background:var(--sheet);
}
.measure {
  min-height:118px;
  padding:22px 3.2vw;
  border-right:1px solid var(--ink);
}
.measure:last-child { border-right:0; }
.measure strong {
  display:block;
  font:400 43px/1 Georgia,"Times New Roman",serif;
}
.measure span {
  display:block;
  margin-top:12px;
  font:700 10px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.1em;
  text-transform:uppercase;
}
.score-section {
  max-width:1500px;
  margin:48px auto 0;
  padding:0 3.2vw;
}
.score-head {
  display:grid;
  grid-template-columns:minmax(0,1fr) auto;
  gap:32px;
  align-items:end;
  margin-bottom:22px;
}
.section-number {
  display:block;
  margin-bottom:5px;
  color:var(--red);
  font:800 11px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
}
h2 {
  margin:0;
  font:400 clamp(34px,4vw,60px)/.9 Georgia,"Times New Roman",serif;
  letter-spacing:-.045em;
}
.score-head p {
  max-width:650px;
  margin:12px 0 0;
  color:var(--muted);
}
.filterbar {
  display:flex;
  justify-content:flex-end;
  gap:7px;
  flex-wrap:wrap;
}
.filter {
  min-height:38px;
  padding:0 13px;
  border:1px solid var(--ink);
  border-radius:0;
  color:var(--ink);
  background:transparent;
  cursor:pointer;
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.06em;
  text-transform:uppercase;
}
.filter:hover { background:#fff; }
.filter.active {
  color:#fff;
  background:var(--ink);
  box-shadow:5px 5px 0 var(--acid);
}
.transport {
  margin-bottom:14px;
  display:grid;
  grid-template-columns:minmax(0,1fr) auto;
  border:2px solid var(--ink);
  background:var(--sheet);
}
.transport-main {
  padding:14px;
  display:flex;
  align-items:end;
  gap:10px;
  flex-wrap:wrap;
}
.scenario-field {
  min-width:260px;
  margin-right:10px;
}
.scenario-field label {
  display:block;
  margin-bottom:6px;
  font:800 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.08em;
  text-transform:uppercase;
}
.scenario-field select {
  width:100%;
  min-height:42px;
  padding:0 38px 0 12px;
  border:1px solid var(--ink);
  border-radius:0;
  color:var(--ink);
  background:var(--sheet);
  font-weight:700;
}
.transport-button {
  min-height:42px;
  padding:0 14px;
  border:1px solid var(--ink);
  border-radius:0;
  color:var(--ink);
  background:transparent;
  cursor:pointer;
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.transport-button:hover { background:var(--acid); }
.transport-button.primary {
  color:#fff;
  background:var(--blue);
  border-color:var(--blue);
}
.transport-button.recovery {
  color:#fff;
  background:var(--red);
  border-color:var(--red);
}
.transport-status {
  min-width:240px;
  padding:13px 16px;
  color:#fff;
  background:var(--ink);
}
.transport-status span,.runtime-cell span {
  display:block;
  color:#99998f;
  font:700 8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.08em;
  text-transform:uppercase;
}
.transport-status strong {
  display:block;
  margin-top:7px;
  color:var(--acid);
  font:800 12px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.service-bridge {
  display:grid;
  grid-template-columns:minmax(175px,.7fr) minmax(220px,1.3fr)
    minmax(160px,.8fr) auto;
  gap:12px;
  align-items:end;
  padding:16px 20px;
  border:1px solid var(--line);
  border-top:0;
  background:rgba(255,255,255,.72);
}
.service-field { display:grid; gap:6px; }
.service-field label {
  color:var(--muted); font:700 .66rem var(--sans);
  letter-spacing:.12em; text-transform:uppercase;
}
.service-field select,.service-field input {
  width:100%; min-height:42px; border:1px solid var(--line);
  border-radius:0; padding:9px 10px; background:var(--paper);
  color:var(--ink); font:700 .78rem var(--sans);
}
.service-connect {
  min-height:42px; border:1px solid var(--ink); padding:8px 16px;
  background:var(--ink); color:var(--paper); cursor:pointer;
  font:800 .72rem var(--sans); letter-spacing:.05em; text-transform:uppercase;
}
.service-state {
  grid-column:1/-1; margin:0; color:var(--muted);
  font:700 .69rem/1.5 var(--mono);
}
.service-state[data-state="live"] { color:#136b47; }
.service-state[data-state="error"] { color:var(--vermilion); }
.progress {
  height:4px;
  margin-top:10px;
  background:#44443e;
}
.progress i {
  width:0;
  height:100%;
  display:block;
  background:var(--acid);
  transition:width 160ms linear;
}
.runtime-readout {
  margin-bottom:14px;
  display:grid;
  grid-template-columns:repeat(4,1fr);
  border:1px solid var(--ink);
  border-bottom:0;
}
.runtime-cell {
  min-width:0;
  min-height:68px;
  padding:13px;
  border-right:1px solid var(--ink);
  border-bottom:1px solid var(--ink);
  background:rgba(255,254,248,.72);
}
.runtime-cell:last-child { border-right:0; }
.runtime-cell strong {
  display:block;
  margin-top:8px;
  overflow:hidden;
  color:var(--ink);
  text-overflow:ellipsis;
  white-space:nowrap;
  font:700 11px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.projection-note {
  grid-column:1/-1;
  margin:0;
  padding:9px 13px;
  border-bottom:1px solid var(--ink);
  color:var(--muted);
  background:#e4e1d5;
  font:600 9px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.score-shell {
  border:2px solid var(--ink);
  background:var(--sheet);
  box-shadow:var(--shadow);
}
.score-scroll {
  overflow-x:auto;
  scrollbar-color:var(--blue) var(--paper);
}
.time-row,.score-row {
  min-width:2130px;
  display:grid;
  grid-template-columns:180px minmax(1950px,1fr);
}
.time-row {
  min-height:36px;
  color:#fff;
  background:var(--ink);
}
.lane-label,.time-label {
  position:sticky;
  left:0;
  z-index:3;
  padding:12px 16px;
  border-right:1px solid var(--ink);
  background:var(--sheet);
}
.time-label {
  color:var(--acid);
  background:var(--ink);
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.time-track {
  display:grid;
  grid-template-columns:repeat(var(--beats),minmax(42px,1fr));
  align-items:center;
}
.time-track span {
  overflow:hidden;
  color:#aaa99f;
  font:600 8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  transform:translateX(-11px) rotate(-45deg);
}
.score-row { min-height:78px; }
.score-row + .score-row { border-top:1px solid var(--hair); }
.lane-label {
  display:flex;
  flex-direction:column;
  justify-content:center;
}
.lane-label strong {
  overflow:hidden;
  text-overflow:ellipsis;
  font:800 12px/1.1 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.lane-label span {
  margin-top:6px;
  color:var(--muted);
  font:700 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.staff {
  --beats:42;
  display:grid;
  grid-template-columns:repeat(var(--beats),minmax(42px,1fr));
  align-items:center;
  padding:7px 0;
  background:
    repeating-linear-gradient(
      to right,
      transparent 0,
      transparent calc((100% / var(--beats)) - 1px),
      rgba(23,23,20,.08) calc((100% / var(--beats)) - 1px),
      rgba(23,23,20,.08) calc(100% / var(--beats))
    ),
    repeating-linear-gradient(
      to bottom,
      transparent 0,
      transparent 10px,
      rgba(23,23,20,.28) 10px,
      rgba(23,23,20,.28) 11px
    );
}
.score-note {
  position:relative;
  z-index:1;
  grid-row:1;
  justify-self:center;
  width:24px;
  height:24px;
  padding:0;
  border:2px solid var(--ink);
  border-radius:50%;
  color:#fff;
  background:var(--ink);
  cursor:pointer;
  transition:transform 150ms ease,box-shadow 150ms ease,opacity 150ms ease;
}
.score-note:before {
  content:"";
  position:absolute;
  left:17px;
  bottom:17px;
  width:2px;
  height:25px;
  background:currentColor;
}
.score-note span {
  position:relative;
  z-index:1;
  font:800 8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.score-note:hover,.score-note[aria-pressed="true"] {
  z-index:2;
  transform:scale(1.42);
  box-shadow:0 0 0 4px var(--sheet),0 0 0 6px var(--ink);
}
.score-note.direction {
  color:#fff;
  border-color:var(--red);
  background:var(--red);
  border-radius:2px 50% 50%;
}
.score-note.coordination {
  color:#fff;
  border-color:var(--blue);
  background:var(--blue);
  border-radius:2px;
}
.score-note.memory {
  color:var(--ink);
  border-color:var(--ink);
  background:var(--acid);
  border-radius:50% 4px 50% 4px;
}
.score-note.execution {
  color:var(--ink);
  background:var(--sheet);
}
.score-note.system {
  color:var(--ink);
  border-color:var(--mint);
  background:var(--mint);
}
.score-note[hidden] { display:none; }
.legend {
  min-height:48px;
  padding:11px 16px;
  display:flex;
  align-items:center;
  flex-wrap:wrap;
  gap:18px;
  border-top:1px solid var(--ink);
  background:#eeece3;
}
.legend span {
  display:flex;
  align-items:center;
  gap:7px;
  font:700 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.legend i {
  width:10px;
  height:10px;
  display:block;
  border:1px solid var(--ink);
  border-radius:50%;
  background:var(--ink);
}
.legend .key-claim { border-radius:1px; background:var(--blue); }
.legend .key-direction { border-radius:1px 50% 50%; background:var(--red); }
.legend .key-memory { border-radius:50% 1px; background:var(--acid); }
.legend .key-execution { background:var(--sheet); }
.below-score {
  max-width:1500px;
  margin:0 auto;
  padding:34px 3.2vw 66px;
  display:grid;
  grid-template-columns:minmax(310px,.75fr) minmax(0,1.25fr);
  gap:28px;
}
.ensemble,.stage-note {
  border-top:5px solid var(--ink);
  padding-top:14px;
}
.mini-title {
  margin:0 0 16px;
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.1em;
  text-transform:uppercase;
}
.performers {
  margin:0;
  padding:0;
  list-style:none;
  border-bottom:1px solid var(--ink);
}
.performers li {
  display:grid;
  grid-template-columns:30px minmax(0,1fr) auto;
  gap:13px;
  align-items:center;
  padding:14px 0;
  border-top:1px solid var(--ink);
}
.part {
  color:var(--blue);
  font:900 18px/1 Georgia,"Times New Roman",serif;
  font-style:italic;
}
.performers strong,.performers small { display:block; }
.performers strong { font-size:13px; }
.performers small {
  overflow:hidden;
  margin-top:2px;
  color:var(--muted);
  text-overflow:ellipsis;
  white-space:nowrap;
  font:600 9px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.performer-status {
  color:#29785f;
  font:800 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.performer-status[data-state="WAITING"] { color:var(--muted); }
.performer-status[data-state="RUNNING"] { color:var(--blue); }
.performer-status[data-state="RECOVERING"] { color:var(--red); }
.budget {
  grid-column:2/4;
  height:3px;
  background:var(--hair);
}
.budget i {
  height:100%;
  display:block;
  background:var(--blue);
}
.stage-note {
  min-height:260px;
  padding:22px;
  color:#fff;
  background:var(--ink);
  border-top-color:var(--red);
}
.stage-note .mini-title { color:var(--acid); }
.event-heading {
  display:grid;
  grid-template-columns:auto 1fr;
  gap:20px;
  align-items:start;
}
.event-number {
  min-width:74px;
  color:var(--acid);
  font:italic 400 72px/.8 Georgia,"Times New Roman",serif;
}
.event-heading h3 {
  margin:0;
  overflow-wrap:anywhere;
  font:400 clamp(26px,3vw,44px)/.95 Georgia,"Times New Roman",serif;
}
.event-heading p {
  margin:9px 0 0;
  color:#adada5;
  font:600 10px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.event-grid {
  margin-top:26px;
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:1px;
  background:#474740;
  border:1px solid #474740;
}
.event-grid div {
  min-width:0;
  padding:11px 13px;
  background:var(--ink);
}
.event-grid span {
  display:block;
  color:#92928a;
  font:700 8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
.event-grid code {
  display:block;
  margin-top:5px;
  overflow-wrap:anywhere;
  color:#fff;
  font:600 10px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.payload {
  grid-column:1/-1;
  max-height:112px;
  overflow:auto;
}
.transcript {
  max-width:1500px;
  margin:0 auto 58px;
  padding:0 3.2vw;
}
.transcript details {
  border-top:1px solid var(--ink);
  border-bottom:1px solid var(--ink);
}
.transcript summary {
  padding:17px 0;
  cursor:pointer;
  font:800 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  letter-spacing:.08em;
  text-transform:uppercase;
}
.table-scroll { overflow:auto; }
table {
  width:100%;
  border-collapse:collapse;
  background:var(--sheet);
  font-size:11px;
}
th,td {
  padding:10px;
  border:1px solid var(--hair);
  text-align:left;
  vertical-align:top;
  white-space:nowrap;
}
th {
  color:#fff;
  background:var(--ink);
  font:700 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
  text-transform:uppercase;
}
td code {
  display:block;
  max-width:520px;
  overflow:hidden;
  color:var(--blue);
  text-overflow:ellipsis;
}
tr[hidden] { display:none; }
.footer {
  padding:24px 3.2vw;
  display:flex;
  justify-content:space-between;
  gap:24px;
  color:#bcbcb4;
  background:var(--ink);
  font:600 10px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;
}
.footer code { color:var(--acid); }
@media (max-width:900px) {
  .opening,.score-head,.below-score { grid-template-columns:1fr; }
  .opening { gap:35px; }
  .opening-note { max-width:600px; }
  .score-head { align-items:start; }
  .filterbar { justify-content:flex-start; }
  .transport { grid-template-columns:1fr; }
  .transport-status { min-width:0; }
  .service-bridge { grid-template-columns:1fr; }
  .service-state { grid-column:1; }
  .runtime-readout { grid-template-columns:1fr 1fr; }
  .runtime-cell:nth-child(2) { border-right:0; }
}
@media (max-width:640px) {
  .masthead { align-items:flex-start; padding:15px 18px; }
  .mast-meta { display:none; }
  .score-nav a:not(:first-child) { display:none; }
  .opening { padding:40px 18px 30px; }
  h1 { font-size:58px; }
  .measure-strip { grid-template-columns:1fr 1fr; }
  .measure { min-height:94px; padding:17px 18px; border-bottom:1px solid var(--ink); }
  .measure:nth-child(even) { border-right:0; }
  .score-section,.below-score,.transcript { padding-left:18px; padding-right:18px; }
  .score-section { margin-top:36px; }
  .scenario-field { width:100%; min-width:0; margin-right:0; }
  .transport-button { flex:1 1 90px; }
  .runtime-readout { grid-template-columns:1fr; }
  .runtime-cell { border-right:0; }
  .time-row,.score-row { grid-template-columns:128px minmax(1950px,1fr); }
  .time-row,.score-row { min-width:2078px; }
  .lane-label,.time-label { padding-left:10px; padding-right:10px; }
  .event-grid { grid-template-columns:1fr; }
  .payload { grid-column:1; }
  .footer { flex-direction:column; padding:22px 18px; }
}
@media (prefers-reduced-motion:reduce) {
  html { scroll-behavior:auto; }
  .score-note { transition:none; }
}
"""
    script = """
const seedEvents=__SEED_EVENTS__;
const seedRuns=__SEED_RUNS__;
const projections={
  office:{
    label:'RECORDED OFFICE / SOURCE REPLAY',
    map:{}
  },
  release:{
    label:'RELEASE TRAIN / DETERMINISTIC PROJECTION',
    map:{
      'docs-agent':'release-notes',
      'deps-agent':'dependency-scout',
      'sre-agent':'release-warden',
      'human:priya':'release-manager',
      'recorded-office':'baton-runtime',
      'memory-distiller':'review-committee'
    }
  },
  incident:{
    label:'INCIDENT CELL / DETERMINISTIC PROJECTION',
    map:{
      'docs-agent':'comms-scribe',
      'deps-agent':'service-investigator',
      'sre-agent':'incident-commander',
      'human:priya':'human-lead',
      'recorded-office':'baton-runtime',
      'memory-distiller':'postmortem-review'
    }
  }
};
const filters=[...document.querySelectorAll('.filter')];
const transcript=[...document.querySelectorAll('tbody tr')];
const fields={
  sequence:document.querySelector('[data-detail="sequence"]'),
  kind:document.querySelector('[data-detail="kind"]'),
  time:document.querySelector('[data-detail="time"]'),
  actor:document.querySelector('[data-detail="actor"]'),
  run:document.querySelector('[data-detail="run"]'),
  payload:document.querySelector('[data-detail="payload"]')
};
const scenario=document.querySelector('#scenario');
const scoreRows=document.querySelector('#scoreRows');
const timeTrack=document.querySelector('#timeTrack');
const performers=document.querySelector('#performers');
const cueState=document.querySelector('#cueState');
const progress=document.querySelector('#progress');
const sourceMode=document.querySelector('#sourceMode');
const serviceEndpoint=document.querySelector('#serviceEndpoint');
const serviceToken=document.querySelector('#serviceToken');
const serviceConnect=document.querySelector('#serviceConnect');
const serviceState=document.querySelector('#serviceState');
const transcriptSection=document.querySelector('.transcript');
serviceEndpoint.value=window.location.origin;
const readout={
  cue:document.querySelector('[data-runtime="cue"]'),
  claims:document.querySelector('[data-runtime="claims"]'),
  advisories:document.querySelector('[data-runtime="advisories"]'),
  objective:document.querySelector('[data-runtime="objective"]')
};
let timer=null;
let activeEvents=seedEvents;
let activeRuns=seedRuns;
let cursor=activeEvents.length;
let mode='REPLAY COMPLETE';
let currentFilter='';
let selectedSequence=activeEvents.length;
function projectedActor(actor){
  return projections[scenario.value].map[actor]||actor;
}
function currentEvents(){
  return activeEvents.map(event=>({...event,actor:projectedActor(event.actor)}));
}
function inspect(event){
  if(!event){
    document.querySelectorAll('.score-note').forEach(note=>{
      note.setAttribute('aria-pressed','false');
    });
    fields.sequence.textContent='—';
    fields.kind.textContent='Waiting for downbeat';
    fields.time.textContent='—';
    fields.actor.textContent='—';
    fields.run.textContent='—';
    fields.payload.textContent='No event has entered the score.';
    return;
  }
  selectedSequence=event.sequence;
  document.querySelectorAll('.score-note').forEach(note=>{
    note.setAttribute('aria-pressed',String(Number(note.dataset.sequence)===event.sequence));
  });
  fields.sequence.textContent=String(event.sequence).padStart(2,'0');
  fields.kind.textContent=event.kind;
  fields.time.textContent=event.time;
  fields.actor.textContent=event.actor;
  fields.run.textContent=event.run;
  fields.payload.textContent=JSON.stringify(event.payload,null,2);
}
function deriveState(events){
  let claims=0;
  let advisories=0;
  let objective='Routine objectives pinned';
  const runStates=Object.fromEntries(activeRuns.map(run=>[run.id,'WAITING']));
  events.forEach(event=>{
    if(event.kind==='CLAIM_GRANTED') claims+=1;
    if(event.kind==='CLAIM_RELEASED') claims=Math.max(0,claims-1);
    if(event.kind==='ADVISORY_PUBLISHED') advisories+=1;
    if(event.kind==='ADVISORY_RETRACTED') advisories=Math.max(0,advisories-1);
    if(event.kind==='OBJECTIVE_UPDATED') objective=event.payload.new||'Objective updated';
    if(event.run!=='fleet'&&runStates[event.run]){
      if(event.kind==='RUN_STARTED'||event.kind==='RUN_RESUMED') runStates[event.run]='RUNNING';
      if(event.kind==='RUN_INTERRUPTED') runStates[event.run]='RECOVERING';
      if(event.kind==='RUN_COMPLETED') runStates[event.run]='COMPLETE';
    }
  });
  return {claims,advisories,objective,runStates};
}
function noteFor(event){
  const button=document.createElement('button');
  button.className=`score-note ${event.family}`;
  button.style.gridColumn=event.sequence;
  button.dataset.kind=event.kind;
  button.dataset.sequence=event.sequence;
  button.setAttribute('aria-label',`Event ${event.sequence}: ${event.kind} by ${event.actor} at ${event.time}`);
  const number=document.createElement('span');
  number.textContent=event.sequence;
  const full=document.createElement('b');
  full.className='sr-only';
  full.textContent=event.kind;
  button.append(number,full);
  button.addEventListener('click',()=>inspect(event));
  return button;
}
function renderPerformers(state){
  performers.replaceChildren();
  activeRuns.forEach((run,index)=>{
    const li=document.createElement('li');
    const part=document.createElement('span');
    part.className='part';
    part.textContent=String(index+1).padStart(2,'0');
    const copy=document.createElement('div');
    const agent=document.createElement('strong');
    agent.textContent=projectedActor(run.agent);
    const id=document.createElement('small');
    id.textContent=run.id;
    copy.append(agent,id);
    const status=document.createElement('span');
    status.className='performer-status';
    status.dataset.state=state.runStates[run.id];
    status.textContent=state.runStates[run.id];
    const budget=document.createElement('div');
    budget.className='budget';
    budget.title=`${run.usedTokens} of ${run.maxTokens} token envelope used in source replay`;
    const usage=document.createElement('i');
    usage.style.width=`${Math.min(100,(run.usedTokens/run.maxTokens)*100)}%`;
    budget.append(usage);
    li.append(part,copy,status,budget);
    performers.append(li);
  });
}
function render(){
  const all=currentEvents();
  const played=all.slice(0,cursor);
  const state=deriveState(played);
  const actors=[...new Set(all.map(event=>event.actor))];
  scoreRows.replaceChildren();
  actors.forEach(actor=>{
    const row=document.createElement('div');
    row.className='score-row';
    const label=document.createElement('div');
    label.className='lane-label';
    const name=document.createElement('strong');
    name.textContent=actor;
    const count=document.createElement('span');
    const playedCount=played.filter(event=>event.actor===actor).length;
    count.textContent=`${playedCount} / ${all.filter(event=>event.actor===actor).length} cues`;
    label.append(name,count);
    const staff=document.createElement('div');
    staff.className='staff';
    staff.style.setProperty('--beats',all.length);
    staff.setAttribute('aria-label',`${actor} event lane`);
    played.filter(event=>event.actor===actor).forEach(event=>{
      const note=noteFor(event);
      note.hidden=Boolean(currentFilter&&!event.kind.includes(currentFilter));
      staff.append(note);
    });
    row.append(label,staff);
    scoreRows.append(row);
  });
  timeTrack.replaceChildren();
  all.forEach((event,index)=>{
    const tick=document.createElement('span');
    tick.textContent=event.time.slice(11,16);
    tick.style.opacity=index<cursor?'1':'.24';
    timeTrack.append(tick);
  });
  const current=played.at(-1);
  readout.cue.textContent=current?`${current.sequence} · ${current.kind}`:'Waiting for downbeat';
  readout.claims.textContent=String(state.claims).padStart(2,'0');
  readout.advisories.textContent=String(state.advisories).padStart(2,'0');
  readout.objective.textContent=state.objective;
  cueState.textContent=`${mode} · ${cursor}/${all.length}`;
  progress.style.width=`${all.length?(cursor/all.length)*100:0}%`;
  renderPerformers(state);
  const selected=played.find(event=>event.sequence===selectedSequence);
  const eligible=[...played].reverse().find(event=>!currentFilter||event.kind.includes(currentFilter));
  inspect(selected&&(!currentFilter||selected.kind.includes(currentFilter))?selected:eligible);
  if(cursor>=all.length&&timer){
    window.clearInterval(timer);
    timer=null;
    mode='REPLAY COMPLETE';
    cueState.textContent=`${mode} · ${cursor}/${all.length}`;
  }
}
function pause(nextMode='PAUSED'){
  if(timer) window.clearInterval(timer);
  timer=null;
  mode=nextMode;
  render();
}
function step(){
  if(cursor<activeEvents.length) cursor+=1;
  selectedSequence=cursor;
  render();
}
function play(nextMode='CONDUCTING'){
  if(cursor>=activeEvents.length) cursor=0;
  if(timer) window.clearInterval(timer);
  mode=nextMode;
  render();
  timer=window.setInterval(step,nextMode==='RECOVERY REPLAY'?520:360);
}
document.querySelector('#start').addEventListener('click',()=>play());
document.querySelector('#pause').addEventListener('click',()=>pause());
document.querySelector('#step').addEventListener('click',()=>{
  pause('STEP MODE');
  step();
});
document.querySelector('#reset').addEventListener('click',()=>{
  pause('RESET / READY');
  cursor=0;
  selectedSequence=0;
  render();
});
document.querySelector('#recover').addEventListener('click',()=>{
  pause('RECOVERY ARMED');
  const index=activeEvents.findIndex(event=>event.kind==='RUN_INTERRUPTED');
  cursor=Math.max(0,index);
  selectedSequence=cursor;
  play('RECOVERY REPLAY');
});
scenario.addEventListener('change',()=>{
  pause(projections[scenario.value].label);
  cursor=0;
  selectedSequence=0;
  render();
});
function familyFor(kind){
  if(kind.includes('CLAIM')) return 'claim';
  if(kind.includes('ADVISORY')||kind.includes('OBJECTIVE')) return 'direction';
  if(kind.includes('LESSON')||kind.includes('EPISODE')) return 'memory';
  if(kind.includes('CHECKPOINT')||kind.includes('TOOL')) return 'execution';
  return 'run';
}
function useFixture(){
  pause('RECORDED OFFICE / SOURCE REPLAY');
  activeEvents=seedEvents;
  activeRuns=seedRuns;
  cursor=activeEvents.length;
  selectedSequence=cursor;
  serviceState.dataset.state='fixture';
  serviceState.textContent='Fixture mode · immutable Python-generated evidence embedded in this page.';
  transcriptSection.hidden=false;
  serviceConnect.textContent='Load source';
  render();
}
async function useLiveService(){
  pause('CONNECTING TO INSTALLED SERVICE');
  const endpoint=serviceEndpoint.value.trim().replace(/\\/$/,'');
  if(!endpoint) throw new Error('Enter a service origin.');
  const headers={Accept:'application/json'};
  if(serviceToken.value) headers.Authorization=`Bearer ${serviceToken.value}`;
  serviceState.dataset.state='fixture';
  serviceState.textContent='Connecting to the installed BATON control plane…';
  const [eventsResponse,runsResponse]=await Promise.all([
    fetch(`${endpoint}/api/v1/events?limit=500`,{headers}),
    fetch(`${endpoint}/api/v1/runs`,{headers})
  ]);
  if(!eventsResponse.ok||!runsResponse.ok){
    throw new Error(`Service returned ${eventsResponse.status}/${runsResponse.status}.`);
  }
  const eventsBody=await eventsResponse.json();
  const runsBody=await runsResponse.json();
  const events=eventsBody.data;
  const runs=runsBody.data;
  if(!Array.isArray(events)||!Array.isArray(runs)){
    throw new Error('Service response did not match BATON API v1.');
  }
  activeEvents=events.map((event,index)=>({
    sequence:index+1,
    sourceSequence:event.sequence,
    time:event.logical_time||'',
    kind:event.kind,
    run:event.run_id||'fleet',
    actor:event.actor||'baton-runtime',
    payload:event.payload||{},
    family:familyFor(event.kind)
  }));
  activeRuns=runs.map(run=>({
    id:run.id,
    agent:run.agent,
    usedTokens:run.used_tokens,
    maxTokens:run.max_tokens
  }));
  cursor=activeEvents.length;
  selectedSequence=cursor;
  mode='INSTALLED SERVICE / DURABLE SQLITE';
  serviceState.dataset.state='live';
  serviceState.textContent=`Live service connected · ${activeRuns.length} runs · ${activeEvents.length} events. Refresh to read new durable state.`;
  transcriptSection.hidden=true;
  serviceConnect.textContent='Refresh live';
  render();
}
sourceMode.addEventListener('change',()=>{
  if(sourceMode.value==='fixture') useFixture();
  else {
    serviceConnect.textContent='Connect live';
    serviceState.dataset.state='fixture';
    serviceState.textContent='Live mode reads the real local API. Run BATON with --static-dir docs for a secure same-origin connection.';
  }
});
serviceConnect.addEventListener('click',async()=>{
  if(sourceMode.value==='fixture'){
    useFixture();
    return;
  }
  try {
    await useLiveService();
  } catch(error) {
    serviceState.dataset.state='error';
    serviceState.textContent=`Connection failed · ${error.message} The embedded fixture remains available.`;
    activeEvents=seedEvents;
    activeRuns=seedRuns;
    cursor=activeEvents.length;
    selectedSequence=cursor;
    transcriptSection.hidden=false;
    mode='REPLAY FALLBACK / SOURCE FIXTURE';
    render();
  }
});
filters.forEach(button=>button.addEventListener('click',()=>{
  filters.forEach(item=>{
    item.classList.remove('active');
    item.setAttribute('aria-pressed','false');
  });
  button.classList.add('active');
  button.setAttribute('aria-pressed','true');
  currentFilter=button.dataset.filter;
  transcript.forEach(row=>row.hidden=Boolean(
    currentFilter&&!row.dataset.kind.includes(currentFilter)
  ));
  render();
}));
render();
""".replace("__SEED_EVENTS__", seed_events_json).replace(
        "__SEED_RUNS__", seed_runs_json
    )
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light">
<title>BATON — the recorded office score</title>
<style>{stylesheet}</style></head><body>
<a class="skip-link" href="#main">Skip to interactive score</a>
<header class="masthead"><div class="wordmark">BATON
<span>Recorded office / score 01</span></div>
<nav class="score-nav" aria-label="Product navigation">
<a href="../">Overview</a><a href="../architecture/">Architecture</a>
<a href="https://github.com/Aneesh-Pothuru/baton">Source</a></nav>
<div class="mast-meta"><span>SQLite / pinned</span>
<span><strong>●</strong> Replay complete</span></div></header>
<main id="main"><section class="opening"><div>
<p class="kicker">Journey 0 · Three agents · One durable record</p>
<h1>The morning,<br><em>conducted.</em></h1></div>
<aside class="opening-note"><p>Not a dashboard of agents. A score of how a
living organization moved—every entrance, claim, warning, handoff, and lesson
held on one shared line of time.</p>
<div class="principles"><span>Leased coordination</span><span>01</span>
<span>Eval-gated memory</span><span>02</span>
<span>Close / reopen recovery</span><span>03</span></div></aside></section>
<section class="measure-strip" aria-label="Performance summary">
<div class="measure"><strong>{len(runs):02d}</strong><span>performers / durable runs</span></div>
<div class="measure"><strong>{claims:02d}</strong><span>claim grants / coordinated</span></div>
<div class="measure"><strong>{advisories:02d}</strong><span>advisories / delivered</span></div>
<div class="measure"><strong>{promoted}:{undetermined}</strong>
<span>memory / promoted : undetermined</span></div></section>
<section class="score-section" aria-labelledby="score-title">
<div class="score-head"><div><span class="section-number">I / THE SCORE</span>
<h2 id="score-title">Every cue, in time.</h2>
<p>Read left to right across the ensemble. Select a note to inspect its complete
durable event; filter the notation without changing the record.</p></div>
<div class="filterbar" role="group" aria-label="Filter event score">
<button type="button" class="filter active" data-filter=""
aria-pressed="true">Full score</button>
<button type="button" class="filter" data-filter="RUN"
aria-pressed="false">Runs</button>
<button type="button" class="filter" data-filter="CLAIM"
aria-pressed="false">Claims</button>
<button type="button" class="filter" data-filter="ADVISORY"
aria-pressed="false">Advisories</button>
<button type="button" class="filter" data-filter="LESSON"
aria-pressed="false">Memory</button></div></div>
<div class="transport" aria-label="Replay controls"><div class="transport-main">
<div class="scenario-field"><label for="scenario">Organization scenario</label>
<select id="scenario"><option value="office">Recorded office · source evidence</option>
<option value="release">Release train · projection</option>
<option value="incident">Incident cell · projection</option></select></div>
<button class="transport-button primary" id="start">Start</button>
<button class="transport-button" id="pause">Pause</button>
<button class="transport-button" id="step">Step</button>
<button class="transport-button" id="reset">Reset</button>
<button class="transport-button recovery" id="recover">Replay recovery</button>
</div><div class="transport-status" role="status"><span>Conductor state</span>
<strong id="cueState">REPLAY COMPLETE</strong><div class="progress">
<i id="progress"></i></div></div></div>
<div class="service-bridge" aria-label="Evidence source">
<div class="service-field"><label for="sourceMode">Evidence source</label>
<select id="sourceMode"><option value="fixture">Embedded source fixture</option>
<option value="live">Installed live service</option></select></div>
<div class="service-field"><label for="serviceEndpoint">BATON service origin</label>
<input id="serviceEndpoint" type="url" value="" placeholder="Same origin by default"
autocomplete="off" spellcheck="false"></div>
<div class="service-field"><label for="serviceToken">API token · if configured</label>
<input id="serviceToken" type="password" value="" placeholder="Kept in memory only"
autocomplete="off" spellcheck="false"></div>
<button class="service-connect" id="serviceConnect" type="button">Load source</button>
<p class="service-state" id="serviceState" data-state="fixture" role="status"
aria-live="polite">Fixture mode ·
immutable Python-generated evidence embedded in this page.</p></div>
<div class="runtime-readout" aria-label="Live organization state">
<p class="projection-note">Fixture mode replays repository evidence; installed
live-service mode reads durable API events generated by the actual runtime.
Alternate organizations are deterministic client-side projections of the same
event semantics; they do not claim additional measured runs.</p>
<div class="runtime-cell"><span>Current cue</span>
<strong data-runtime="cue">—</strong></div>
<div class="runtime-cell"><span>Active claims</span>
<strong data-runtime="claims">00</strong></div>
<div class="runtime-cell"><span>Live advisories</span>
<strong data-runtime="advisories">00</strong></div>
<div class="runtime-cell"><span>Pinned objective</span>
<strong data-runtime="objective">Routine objectives pinned</strong></div></div>
<div class="score-shell"><div class="score-scroll" tabindex="0"
aria-label="Horizontally scrollable Fleet timeline conductor score">
<div class="time-row"><div class="time-label">logical time →</div>
<div class="time-track" id="timeTrack" style="--beats:{event_count}">{time_cells}</div></div>
<div id="scoreRows">{score_lanes}</div></div>
<div class="legend" aria-label="Score notation legend">
<span><i></i>run / routine</span><span><i class="key-claim"></i>claim</span>
<span><i class="key-direction"></i>direction</span>
<span><i class="key-memory"></i>memory</span>
<span><i class="key-execution"></i>checkpoint / tool</span></div></div></section>
<section class="below-score"><section class="ensemble">
<p class="mini-title">II / Ensemble on duty</p><ol class="performers"
id="performers">{performer_rows}</ol>
</section><aside class="stage-note" aria-live="polite">
<p class="mini-title">Selected stage note</p><div class="event-heading">
<strong class="event-number" data-detail="sequence">01</strong><div>
<h3 data-detail="kind">Select a note</h3>
<p>append-only event / exact provenance</p></div></div>
<div class="event-grid"><div><span>Logical time</span>
<code data-detail="time">—</code></div><div><span>Actor</span>
<code data-detail="actor">—</code></div><div><span>Run</span>
<code data-detail="run">—</code></div><div class="payload"><span>Payload</span>
<code data-detail="payload">—</code></div></div></aside></section>
<section class="transcript"><details><summary>III / Open the complete event transcript
· {len(events)} immutable entries</summary><div class="table-scroll"><table>
<thead><tr><th>#</th><th>Logical time</th><th>Actor</th><th>Event</th>
<th>Run</th><th>Complete payload</th></tr></thead>
<tbody>{transcript_rows}</tbody></table></div></details></section></main>
<footer class="footer"><span>Fixture-derived implementation evidence,
not a reliability benchmark.</span><span>Generated by <code>make demo</code>
· SQLite retains complete payloads.</span></footer><script>{script}</script>
</body></html>
"""
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(document, encoding="utf-8")
    return target
