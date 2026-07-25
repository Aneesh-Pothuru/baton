# BATON user journeys

The product has two primary surfaces:

- the **landing site** at `docs/index.html`, which explains the thesis,
  implementation proof, architecture, and honest limits;
- the **interactive score** at `docs/demo/index.html`, which conducts the
  deterministic source replay and its clearly labeled client-side projections;
- the **installed control plane**, which persists real routines, runs, claims,
  advisories, handoffs, checkpoints, and events and can feed the score live.

## 1. First-time platform lead

**Question:** Is this a credible organizational runtime or an agent-themed
dashboard?

1. Enter through the landing hero, then scan the four-item proof ribbon.
2. Read **The organizational gap** to distinguish claims, pinned handoffs, and
   eval-gated memory from generic agent execution.
3. Inspect **The interface leads back to artifacts** for the concrete Journey 0
   evidence.
4. Open **Architecture** and review the five-layer flow plus the explicit MVP
   limits.
5. Select **Conduct the live replay** and use **Step** to observe state change
   one event at a time.

**Success state:** The lead can name the durability boundary, identify which
claims are implemented, and trace a source event to actor, run, time, and
payload.

**Recovery/failure state:** If the scope seems broader than the proof, the
**Honest boundary** and architecture limits identify single-node coordination,
recorded agents, fixture evaluation, and client-side projections as unmeasured
or deliberately constrained.

## 2. Agent operator coordinating work

**Question:** Who owns the work, which direction is active, and what happens
next?

1. Open the interactive score and choose the **Recorded office** organization.
2. Select **Reset**, then **Start** or advance with **Step**.
3. Watch **Active claims**, **Live advisories**, and **Pinned objective** update
   as cues enter the score.
4. Filter to **Claims** to inspect queue, grant, release, and transfer events.
5. Filter to **Advisories** and select the vermilion notes to inspect delivery,
   reaction, and retraction payloads.
6. Switch to **Release train** to see the same deterministic organizational
   semantics projected onto different roles.

**Success state:** The operator can identify the current claim count, advisory
count, objective, actor lane, and exact event that changed each one.

**Recovery/failure state:** On a conflict or stale direction, pause the score,
inspect the selected note, and open the complete source transcript. Projection
labels prevent an alternate organization from being mistaken for new measured
evidence.

## 3. Incident responder replaying recovery

**Question:** Did the organization really resume from durable state?

1. Open the interactive score and choose **Incident cell** for a role-oriented
   projection, or stay on **Recorded office** for source actor names.
2. Select **Replay recovery**.
3. Observe the conductor jump to the interruption edge and replay
   `RUN_INTERRUPTED`, `RUN_RESUMED`, advisory retraction, the next checkpoint,
   claim release, and final completion.
4. Select the recovery cues and compare actor, run ID, logical time, and payload
   in **Selected stage note**.
5. Open the full transcript to verify the immutable source ordering.
6. Return to the landing **Recovery** evidence item or architecture
   **Resume from the durable edge** contract for the implementation boundary.

**Success state:** The responder sees a visible recovering state followed by a
running/completed state and can identify the exact checkpoint-adjacent events.

**Recovery/failure state:** Pause at any unexpected cue, reset the replay, and
step manually. If stronger reliability evidence is required, the product
directs the responder to `make reproduce-resume` and avoids presenting the
static projection as a benchmark.

## 4. Researcher inspecting lesson promotion

**Question:** What evidence allowed organizational memory to change?

1. Start on the landing **Memory hygiene** evidence item.
2. Open the score, filter to **Memory**, and reset/step or run the full replay.
3. Select `EPISODE_WRITTEN` cues to inspect their source tasks and outcomes.
4. Select `LESSON_GATE_EVALUATED` to inspect the lesson ID, source episodes,
   registered mean delta, win rate, gate result, and final status.
5. Open the complete transcript and compare the memory event with the source
   episodes.
6. Read the architecture **Abstention is a valid verdict** contract.

**Success state:** The researcher can explain why the registered fixture lesson
was promoted and can locate every input/output identifier in the durable
record.

**Recovery/failure state:** Missing, non-finite, mismatched, or empty comparison
evidence produces `UNDETERMINED`; non-positive evidence is archived. The UI
does not silently convert absence of evidence into success.

## 5. Integrator running the installed product

**Question:** Can BATON coordinate real caller-owned loops and survive restart?

1. Install the wheel and start `baton serve` on loopback, optionally serving
   `docs` from the same process.
2. Register a routine through `POST /api/v1/routines`.
3. Fire it; BATON starts the durable run, pins constraints, creates the initial
   checkpoint, subscribes the run, and acquires its declared work claims.
4. A caller-owned agent loop submits usage and tool-boundary checkpoints.
5. An SRE integration publishes an advisory; the caller asks BATON to deliver
   it at the next step boundary and records its own reaction.
6. A human integration updates the objective with an explicit issuer.
7. Restart the service, resume the run, and fetch its complete evidence bundle.
8. In the score, select **Installed live service** to inspect current API events
   instead of the embedded fixture.

**Success state:** SQLite survives restart; the restored workspace reference,
harness state, pinned constraints, advisory state, and objective match the
latest checkpoint and handoff versions.

**Recovery/failure state:** Invalid JSON and state transitions return structured
4xx errors. Non-loopback startup without a token fails closed. If live mode
cannot connect, the score names the failure and returns to the immutable replay
without presenting it as live evidence.
