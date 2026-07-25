# BATON local control plane

The installed product includes a dependency-free HTTP service over the same
SQLite runtime exercised by `make demo`. It is designed for a single operator
or a trusted local automation layer. The service is not a hosted multi-tenant
control plane.

## Start securely

Loopback is the default, so a local service needs no token:

```bash
baton serve --database state/baton.sqlite --static-dir docs
```

Open `http://127.0.0.1:8020/demo/`, select **Installed live service**, and
connect. The score reads the real durable run and event APIs. The embedded
fixture remains available even if the service is offline.

Any non-loopback bind fails closed unless `BATON_API_TOKEN` is set. Compose
binds the published port to host loopback and still requires a token inside the
container:

```bash
cp .env.example .env
python -c 'import secrets; print(secrets.token_urlsafe(32))'
# Put the printed value in .env, then:
docker compose up --build
```

Enter that token in the score's in-memory token field, or send it as
`Authorization: Bearer …`. Health and readiness remain unauthenticated for
container probes.

## API surface

All application responses use either
`{"data": ..., "meta": {"request_id": ...}}` or an explicit `error` object.
POST requests require `application/json`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz`, `/readyz` | Process and SQLite readiness |
| GET/POST | `/api/v1/routines` | List or register durable routine specs |
| POST | `/api/v1/routines/{name}/fire` | Start a run and acquire declared claims |
| GET | `/api/v1/runs` | List durable runs |
| GET | `/api/v1/runs/{id}` | Current run, pinned handoff, latest checkpoint |
| GET | `/api/v1/runs/{id}/evidence` | Ordered events, checkpoints, and handoffs |
| POST | `/api/v1/runs/{id}/checkpoint` | Persist a tool-boundary recovery edge |
| POST | `/api/v1/runs/{id}/tool-boundary` | Record tool result and its recovery edge |
| POST | `/api/v1/runs/{id}/resume` | Restore latest checkpoint and pinned handoff |
| POST | `/api/v1/runs/{id}/objective` | Apply an attributed objective update |
| POST | `/api/v1/runs/{id}/usage` | Enforce token, cost, and wall-clock budgets |
| POST | `/api/v1/runs/{id}/deliver-advisories` | Inject subscribed advisories at a step |
| POST | `/api/v1/runs/{id}/advisory-reaction` | Record the caller-owned loop's reaction |
| POST | `/api/v1/runs/{id}/complete` | Record a terminal outcome |
| GET/POST | `/api/v1/claims` | Inspect or acquire TTL work claims |
| POST | `/api/v1/claims/{scope}/renew` | Renew with the owner token |
| POST | `/api/v1/claims/{scope}/release` | Compare-and-delete and grant next waiter |
| GET/POST | `/api/v1/advisories` | Inspect or publish scoped advisories |
| POST | `/api/v1/advisories/{id}/retract` | Retract as the original publisher |
| GET | `/api/v1/events?run_id=&limit=` | Read the append-only fleet event stream |
| GET | `/api/v1/lessons` | Inspect lesson gate outcomes and provenance |
| GET/POST | `/api/v1/episodes` | Inspect or write attributed run episodes |
| POST | `/api/v1/lessons/gate` | Promote, archive, or abstain from a candidate |

## Minimal installed journey

Register:

```bash
curl -sS http://127.0.0.1:8020/api/v1/routines \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"morning-release",
    "agent":"release-agent",
    "schedule":"0 7 * * 1-5",
    "objective":"Prepare a release without bypassing CI.",
    "constraints":["Never deploy without human approval."],
    "budget":{"max_tokens":5000,"max_cost":1,"max_wall_seconds":1800},
    "coordination":{"claims":["repo:api"],"subscribe":["infra.*"]},
    "workspace_ref":"git:api@main",
    "harness_state":{"next":"inspect-ci"}
  }'
```

Fire and inspect:

```bash
curl -sS http://127.0.0.1:8020/api/v1/routines/morning-release/fire \
  -H 'Content-Type: application/json' \
  -d '{"run_id":"release-001"}'
curl -sS http://127.0.0.1:8020/api/v1/runs/release-001/evidence
```

The HTTP service coordinates durable state. It deliberately does not execute a
model or arbitrary tools; callers run their own loop and write real tool
boundaries, usage, reactions, and completion through the API.
