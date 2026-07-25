# Competitive UX and design thesis

Reviewed 2026-07-24 against orchestration, agent debugging, musical timeline,
and real mission-control interfaces. These are interaction references, not
feature-parity claims.

| Source | Relevant surface | Pattern retained |
| --- | --- | --- |
| [Ableton Live Arrangement View](https://www.ableton.com/en/manual/arrangement-view/) | stacked tracks on a shared horizontal timeline | BATON gives each organizational actor a lane and keeps time moving left-to-right, so simultaneous work remains spatially comparable. |
| [Ableton accessibility and keyboard navigation](https://www.ableton.com/en/live-manual/12/accessibility-and-keyboard-navigation/) | keyboard movement across tracks and timeline | Every cue is a native button, the horizontal score is keyboard-focusable, transport controls are labeled, and the full source transcript remains available. |
| [LangSmith Studio](https://docs.langchain.com/langsmith/studio) | graph/state inspection and agent time travel | A selected cue exposes its exact actor, run, logical time, event kind, and payload instead of opening an unrelated log surface. |
| [LangGraph time travel](https://docs.langchain.com/oss/python/langgraph/use-time-travel) | replay and fork from checkpoints | Recovery replay begins at the interruption edge and visibly conducts the resume sequence forward. BATON does not imply that a projection is a new measured run. |
| [Prefect states](https://docs.prefect.io/v3/concepts/states) | explicit lifecycle states and state history | Waiting, running, recovering, and complete states are treated as operational semantics rather than decorative status colors. |
| [CrewAI AMP](https://docs.crewai.com/enterprise/introduction) | multi-agent deployment, monitoring, and live execution | Scenario choice and ensemble state occupy one control surface, but BATON centers the shared organizational record rather than individual agent cards. |
| [NASA Mission Control](https://www.nasa.gov/johnson/jsc-mission-control-center/) | specialized controllers led by a flight director | Actors are presented as an ensemble of accountable positions. The conductor metaphor is functional: one shared score coordinates specialized roles. |
| [NASA Artemis I Mission Control](https://www.nasa.gov/missions/artemis/orion/artemis-i-mission-control-at-a-glance/) | named console positions and phase-specific responsibility | Lane labels stay stable while event notation changes, preserving “who owns what” through dynamic phases. |

## Rejected default

The previous interface used the same dark panels, metric cards, status pills,
and event table as the other thesis demos. That treatment made BATON look like
a generic observability dashboard and hid its strongest idea: an organization
is a set of roles moving together across durable time.

## Chosen design thesis: the organizational score

BATON is a **spatial mission-control score for a living agent organization**.
Its interface is deliberately light, editorial, and typographic rather than a
dark SaaS dashboard:

- warm score paper, black notation, cobalt coordination, vermilion direction,
  and acid-yellow memory;
- a serif editorial voice paired with terse monospaced operational labels;
- horizontal time, stable actor lanes, and distinct note shapes for run,
  claim, advisory, memory, and checkpoint events;
- a transport for start, pause, step, reset, and recovery replay;
- source evidence as the default scenario, with alternate organizations clearly
  labeled as deterministic client-side projections of the same event semantics;
- a selected stage note with complete provenance and an expandable immutable
  transcript for verification;
- landing, architecture, evidence, limits, and interactive score surfaces that
  share a product-specific visual grammar.

## Product and accessibility constraints

- GitHub Pages only: no server, model key, remote font, or runtime dependency.
- The Python/SQLite implementation remains the source of truth; generated HTML
  embeds the exact 42-event replay.
- Color never carries state alone. Cue shape, lane, event text, and state label
  carry the same meaning.
- Controls use native buttons/selects, selected events expose `aria-pressed`,
  live state uses a polite status region, and reduced-motion preferences remove
  cue animation.
- The score is horizontally scrollable by design; the layout stacks controls
  and inspection surfaces on narrow screens without collapsing provenance.
