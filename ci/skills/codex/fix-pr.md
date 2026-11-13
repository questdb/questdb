=== frontmatter
---
name: fix-pr
description: {{description}}
---
=== harness-note
> Harness note: this skill runs inside **Codex**. Helpers are fresh-context Codex
> subagents, the ones with a role of the agent types in `.codex/agents/`. Shell variables don't carry over between shell calls, so write each
> value Setup records, such as `START` or the PR base, literally into later commands
> and task texts.

=== helpers
Launch helpers one at a time with `spawn_agent`, using `fork_turns: "none"`,
`model: "<HELPER_MODEL>"` (Setup), the agent type and effort below, and the task
templates at the end of this file. Wait for each one with `wait_agent`, in long
waits, before the next step.

Every agent of the run thinks at the highest effort, `xhigh`, except the
review's discovery agents: review-pr runs them as `reviewer` at `high`, because
they only read code. A definition's own effort overrides the `reasoning_effort`
a launch passes, so `reviewer` sets none and its callers choose;
`review-pr-runner`, `risk-assessor` and the review's `review-prober` falsifiers
set `xhigh` in their definitions in `.codex/agents/`.

| Helper | `agent_type` | `reasoning_effort` |
|---|---|---|
| Analysis | none | `"xhigh"` |
| Check | `reviewer` | `"xhigh"` |
| Build | none | `"xhigh"` |
| Test review | `reviewer` | `"xhigh"` |
| Test gaps | none | `"xhigh"` |
| Risk | `risk-assessor` | none |
| Review | `review-pr-runner` | none |

=== setup-model
   model, which is this session's model, and this session's effort; both
   are in the session's rollout file:
   `f=$(find "${CODEX_HOME:-$HOME/.codex}/sessions" -name "rollout-*-$CODEX_THREAD_ID.jsonl" | head -1); grep '"type":"turn_context"' "$f" | tail -1 | jq -r '.payload | "\(.model) \(.effort)"'`.
=== setup-effort-check
   If this session's effort is not `xhigh`, say so and ask too.
=== helper-questions
- A helper may ask the main session a question mid-task, in a message to
  `/root`; answer with `send_message`. Answer only with facts from its task,
  the state directory or the user. Never approve a change to the design -
  new logic, another approach - since the main session hasn't read the code:
  tell the helper to stop and give the reason in its reply (`STOPPED`, or
  `ASK` or `BLOCK` from the analysis), and handle that reply as usual. Only
  the analysis and the check decide a design.
