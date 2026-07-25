# Competitive UI review

Reviewed 2026-07-24 against durable-execution and agent-control interfaces.

| Product | Relevant surface | What works |
| --- | --- | --- |
| [CrewAI](https://crewai.com/) | multi-agent control plane | Agents, workflow state, governance, and intervention are presented as one operational system. |
| [LangGraph Studio](https://www.langchain.com/blog/langgraph-studio-the-first-agent-ide) | stateful agent debugging | Graph structure and the current execution state stay visually connected. |
| [Inngest](https://www.inngest.com/docs/platform/monitor/traces) | durable run traces | A proportional step timeline on the left and contextual detail on the right make retries legible. |
| [Temporal](https://docs.temporal.io/visibility) | workflow visibility | Searchable workflow state and durable history make recovery feel like normal operations, not an exception. |

## Direction adopted

- Present the morning as a live organization: agents, claims, advisories, and
  memory promotion share one control-room canvas.
- Show fleet health and governance in the top rail before the event ledger.
- Render agent runs as compact identity cards with status and durable run IDs.
- Keep the append-only timeline dense, scan-friendly, and filterable by event
  type without hiding the underlying record.
- Use ultraviolet for coordination, cyan for active work, and green only for
  durable success.

The result is an agent operations console rather than a cron log viewer.
