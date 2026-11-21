=== frontmatter
---
name: fix-pr
description: {{description}}
disable-model-invocation: true
allowed-tools: bash read edit write subagent ctx_execute ctx_execute_file
metadata:
  argument-hint: "{{argument_hint}}"
---
=== harness-note
=== helpers
Launch helpers one at a time as ordinary foreground `subagent` calls (never
`async: true`) with `context: "fresh"`, `model: "<HELPER_MODEL>"` (Setup) and
the task templates at the end of this file. While the main session waits
inside a tool call, pi keeps its prompt cache warm; a session that ends its
turn and is woken later by a background helper loses the cache and pays to
rebuild it.

Every agent of the run thinks at max, except the review's discovery agents:
review-pr runs them as `review-reader` (`.pi/agents/review-reader.md`) at its default,
high, because they only read code. The builtin `worker` and the `review-reader`
definition default to high, and a launch overrides that only through the `:max` suffix of its
model;
`review-pr-runner`, `risk-assessor` and the review's `review-prober`
falsifiers set max in their definitions. The timeouts leave room for it.

| Helper | Agent | Extra parameters |
|---|---|---|
| Analysis | `worker` | `timeoutMs: 7200000`, `acceptance: { level: "none", reason: "fix-pr checks the result" }` |
| Check | `review-reader` | `timeoutMs: 3600000` |
| Build | `worker` | `timeoutMs: 10800000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |
| Test review | `review-reader` | `timeoutMs: 3600000` |
| Test gaps | `worker` | `timeoutMs: 5400000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |
| Risk | `risk-assessor` | `timeoutMs: 3600000` |
| Review | `review-pr-runner` | `timeoutMs: 28800000`, `acceptance: { level: "none", reason: "fix-pr reads the report" }` |

=== setup-model
   model too: `HELPER_MODEL="$PI_PROVIDER/$PI_MODEL:max"`, this session's
   model at max thinking.
=== setup-effort-check
   If this session doesn't think at max (`$PI_REASONING_LEVEL`), say so and ask too.
=== helper-questions
- A helper may ask the main session a question mid-task
  (`contact_supervisor`). Answer only with facts from its task, the state
  directory or the user. Never approve a change to the design - new logic,
  another approach - since the main session hasn't read the code: tell the
  helper to stop and give the reason in its reply (`STOPPED`, or `ASK` or
  `BLOCK` from the analysis), and handle that reply as usual. Only the
  analysis and the check decide a design.
