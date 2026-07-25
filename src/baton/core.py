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
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
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
            active = None
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
        valid = (
            bool(baseline_scores)
            and len(baseline_scores) == len(candidate_scores)
            and all(math.isfinite(value) for value in baseline_scores)
            and all(math.isfinite(value) for value in candidate_scores)
            and bool(source_episode_ids)
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
    """Render all durable organization events into one static HTML timeline."""

    events = runtime.rows("events")
    runs = runtime.rows("runs")
    lessons = runtime.rows("lessons")
    promoted = sum(row["status"] == "PROMOTED" for row in lessons)
    undetermined = sum(row["status"] == "UNDETERMINED" for row in lessons)
    claims = sum(row["kind"] == "CLAIM_GRANTED" for row in events)
    advisories = sum(row["kind"] == "ADVISORY_PUBLISHED" for row in events)
    event_rows = "\n".join(
        f"<tr data-kind='{html.escape(str(row['kind']))}'>"
        f"<td>{row['sequence']}</td>"
        f"<td>{html.escape(str(row['logical_time']))}</td>"
        f"<td>{html.escape(str(row['actor'] or 'control-plane'))}</td>"
        f"<td><span class='kind'>{html.escape(str(row['kind']))}</span></td>"
        f"<td>{html.escape(str(row['run_id'] or 'fleet'))}</td>"
        "</tr>"
        for row in events
    )
    run_cards = "\n".join(
        "<article class='agent-card'>"
        f"<div class='avatar'>{html.escape(str(row['agent'])[:2].upper())}</div>"
        "<div class='agent-copy'>"
        f"<p>{html.escape(str(row['agent']))}</p>"
        f"<strong><i></i>{html.escape(str(row['status']))}</strong>"
        f"<small>{html.escape(str(row['id']))}</small></div>"
        "</article>"
        for row in runs
    )
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BATON — recorded office timeline</title>
<style>
:root{{--bg:#070910;--panel:#0e111d;--panel-2:#131728;--line:#252a42;
--text:#f7f7ff;--muted:#8d94ad;--faint:#626980;--violet:#9a86ff;
--violet-2:#6655e8;--cyan:#67d8ef;--green:#68e6a2;--amber:#f1c76d;
--shadow:0 26px 76px rgba(0,0,0,.42)}}
*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;color:var(--text);
background:radial-gradient(circle at 75% 0,rgba(102,85,232,.2),transparent 32rem),
radial-gradient(circle at 12% 30%,rgba(103,216,239,.07),transparent 26rem),var(--bg);
font:14px/1.55 Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
button{{font:inherit}}.topbar{{height:58px;padding:0 26px;display:flex;align-items:center;
justify-content:space-between;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:10;
background:rgba(7,9,16,.86);backdrop-filter:blur(16px)}}.brand{{display:flex;align-items:center;
gap:11px;font-weight:780;letter-spacing:.02em}}.brand-mark{{width:27px;height:27px;
display:grid;place-items:center;border-radius:8px;background:linear-gradient(135deg,var(--violet),
var(--violet-2));color:#080911;box-shadow:0 0 24px rgba(154,134,255,.28)}}.topmeta{{
display:flex;align-items:center;gap:15px;color:var(--muted);font:10px ui-monospace,
SFMono-Regular,Menlo,monospace;text-transform:uppercase;letter-spacing:.11em}}.online{{
display:flex;gap:7px;align-items:center;color:#c6f4da}}.online:before{{content:"";width:6px;height:6px;
border-radius:50%;background:var(--green);box-shadow:0 0 12px var(--green)}}main{{
max-width:1380px;margin:auto;padding:30px 26px 64px}}.hero{{display:grid;
grid-template-columns:minmax(0,1.15fr) minmax(360px,.85fr);border:1px solid var(--line);
border-radius:18px;overflow:hidden;background:linear-gradient(145deg,rgba(20,24,42,.97),
rgba(11,14,25,.98));box-shadow:var(--shadow)}}.hero-copy{{padding:36px 38px}}.eyebrow,
.section-label{{margin:0 0 13px;color:var(--violet);font:750 10px ui-monospace,
SFMono-Regular,Menlo,monospace;text-transform:uppercase;letter-spacing:.16em}}.eyebrow:before{{
content:"";display:inline-block;width:24px;height:1px;background:currentColor;vertical-align:middle;
margin-right:9px}}h1{{font-size:clamp(38px,5vw,66px);line-height:.98;letter-spacing:-.05em;
margin:0;max-width:760px}}h2{{font-size:21px;letter-spacing:-.02em}}.lede{{font-size:17px;
color:#b4bad0;max-width:720px;margin:20px 0 0}}.control-panel{{border-left:1px solid var(--line);
padding:28px;background:rgba(7,9,16,.28)}}.control-head{{display:flex;align-items:center;
justify-content:space-between;margin-bottom:18px}}.control-head span{{color:var(--cyan);
font:10px ui-monospace,monospace}}.pulse{{height:2px;background:var(--line);position:relative;
margin:16px 0 24px}}.pulse:after{{content:"";position:absolute;left:0;top:0;height:100%;width:72%;
background:linear-gradient(90deg,var(--violet),var(--cyan));box-shadow:0 0 14px var(--violet)}}
.control-row{{display:grid;grid-template-columns:1fr auto;gap:18px;padding:11px 0;
border-bottom:1px solid var(--line)}}.control-row span{{color:var(--muted)}}.control-row strong{{
font:700 11px ui-monospace,monospace}}.metric-grid{{display:grid;grid-template-columns:
repeat(4,minmax(0,1fr));gap:12px;margin:17px 0}}.metric{{border:1px solid var(--line);
border-radius:13px;background:rgba(14,17,29,.9);padding:17px}}.metric strong{{display:block;
font-size:26px;line-height:1.15}}.metric span{{display:block;color:var(--muted);margin-top:7px;
font-size:10px;text-transform:uppercase;letter-spacing:.1em}}.workspace{{display:grid;
grid-template-columns:minmax(250px,.32fr) minmax(0,1fr);gap:14px}}.panel{{border:1px solid var(--line);
border-radius:15px;background:linear-gradient(180deg,rgba(17,21,36,.97),rgba(10,13,23,.98));
overflow:hidden}}.panel-head{{padding:18px 20px;border-bottom:1px solid var(--line);
display:flex;align-items:center;justify-content:space-between;gap:16px}}.panel-head h2{{margin:0}}
.panel-head p{{margin:3px 0 0;color:var(--muted);font-size:12px}}.agent-list{{padding:12px}}
.agent-card{{display:flex;align-items:center;gap:12px;padding:13px;border:1px solid transparent;
border-radius:11px}}.agent-card+ .agent-card{{border-top-color:var(--line);border-radius:0}}
.avatar{{width:38px;height:38px;flex:0 0 auto;border-radius:11px;display:grid;place-items:center;
background:linear-gradient(145deg,#242b48,#191e34);border:1px solid #343c61;color:#dcd7ff;
font:750 11px ui-monospace,monospace}}.agent-copy{{min-width:0}}.agent-copy p{{margin:0;
font-weight:700}}.agent-copy strong{{display:flex;align-items:center;gap:6px;color:var(--green);
font:650 10px ui-monospace,monospace;text-transform:uppercase}}.agent-copy strong i{{
width:5px;height:5px;border-radius:50%;background:currentColor}}.agent-copy small{{display:block;
color:var(--faint);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font:10px
ui-monospace,monospace}}.filterbar{{display:flex;gap:6px;flex-wrap:wrap}}.filter{{border:1px solid
var(--line);border-radius:7px;padding:6px 9px;background:#0c0f1b;color:var(--muted);
cursor:pointer;font:650 9px ui-monospace,monospace;text-transform:uppercase;letter-spacing:.08em}}
.filter:hover,.filter:focus-visible,.filter.active{{color:#fff;border-color:#574c9d;
background:#241f48;outline:none}}.scroll{{overflow:auto;max-height:690px}}table{{width:100%;
border-collapse:collapse;font-size:12px}}th,td{{text-align:left;padding:11px 13px;
border-bottom:1px solid var(--line);white-space:nowrap}}th{{position:sticky;top:0;z-index:1;
background:#101421;color:var(--faint);font:700 9px ui-monospace,monospace;text-transform:uppercase;
letter-spacing:.1em}}tbody tr:hover{{background:rgba(103,216,239,.035)}}.kind{{font:650 9px
ui-monospace,monospace;background:#201c42;color:#c6bcff;border:1px solid #393267;padding:4px 7px;
border-radius:6px}}.note{{display:flex;justify-content:space-between;gap:20px;margin-top:20px;
padding-top:16px;border-top:1px solid var(--line);color:var(--muted);font-size:12px}}code{{
color:#c8c2ff}}@media(max-width:980px){{.hero,.workspace{{grid-template-columns:1fr}}
.control-panel{{border-left:0;border-top:1px solid var(--line)}}.metric-grid{{
grid-template-columns:1fr 1fr}}}}@media(max-width:640px){{.topbar{{padding:0 14px}}.topmeta>span:first-child{{
display:none}}main{{padding:18px 14px 42px}}.hero-copy,.control-panel{{padding:24px 20px}}
.metric-grid{{grid-template-columns:1fr 1fr}}.panel-head{{align-items:start;flex-direction:column}}
.note{{flex-direction:column}}}}@media(prefers-reduced-motion:reduce){{html{{scroll-behavior:auto}}}}
</style></head><body>
<header class="topbar"><div class="brand"><span class="brand-mark">B</span>BATON</div>
<div class="topmeta"><span>fleet control / morning routine</span><span class="online">replay complete</span>
</div></header><main><section class="hero"><div class="hero-copy">
<p class="eyebrow">Organization runtime · Journey 0</p><h1>A legible morning.</h1>
<p class="lede">Three durable agents coordinate claims, advisories, objectives,
budgets, and memory through one append-only operating record.</p></div>
<aside class="control-panel"><div class="control-head"><p class="section-label">Control plane</p>
<span>SESSION 07:30—09:10</span></div><div class="pulse"></div>
<div class="control-row"><span>Durability</span><strong>SQLITE / PINNED</strong></div>
<div class="control-row"><span>Coordination</span><strong>LEASED</strong></div>
<div class="control-row"><span>Memory gate</span><strong>EVAL REQUIRED</strong></div>
<div class="control-row"><span>Recovery</span><strong>CLOSE / REOPEN</strong></div></aside></section>
<section class="metric-grid" aria-label="Fleet metrics">
<div class="metric"><strong>{len(runs)}</strong><span>durable runs</span></div>
<div class="metric"><strong>{claims}</strong><span>claim grants</span></div>
<div class="metric"><strong>{advisories}</strong><span>advisories</span></div>
<div class="metric"><strong>{promoted}/{undetermined}</strong><span>promoted / undetermined</span></div>
</section><section class="workspace"><aside class="panel"><div class="panel-head"><div>
<p class="section-label">Fleet roster</p><h2>Agents on duty</h2></div></div>
<div class="agent-list">{run_cards}</div></aside><section class="panel">
<div class="panel-head"><div><p class="section-label">Append-only event ledger</p>
<h2>Fleet timeline</h2><p>Filter the view without changing the durable record.</p></div>
<div class="filterbar" role="group" aria-label="Filter event timeline">
<button class="filter active" data-filter="">All</button>
<button class="filter" data-filter="RUN">Runs</button>
<button class="filter" data-filter="CLAIM">Claims</button>
<button class="filter" data-filter="ADVISORY">Advisories</button>
<button class="filter" data-filter="LESSON">Memory</button></div></div>
<div class="scroll"><table>
<thead><tr><th>#</th><th>Logical time</th><th>Actor</th><th>Event</th><th>Run</th></tr></thead>
<tbody>{event_rows}</tbody></table></div></section></section>
<footer class="note"><span>Fixture-derived implementation evidence, not a reliability benchmark.</span>
<span>Generated by <code>make demo</code> · SQLite retains complete payloads.</span></footer>
</main><script>
const buttons=[...document.querySelectorAll('.filter')];
const rows=[...document.querySelectorAll('tbody tr')];
buttons.forEach(button=>button.addEventListener('click',()=>{{
  buttons.forEach(item=>item.classList.remove('active'));
  button.classList.add('active');
  const filter=button.dataset.filter;
  rows.forEach(row=>row.hidden=filter && !row.dataset.kind.includes(filter));
}}));
</script></body></html>
"""
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(document, encoding="utf-8")
    return target
