=== frontmatter
---
name: fix-pr
description: {{description}}
argument-hint: "{{argument_hint}}"
disable-model-invocation: true
allowed-tools: Bash, Read, Edit, Write, Grep, Glob, Agent, AskUserQuestion
---
=== harness-note
> Harness note: this skill runs inside **Claude Code**. Helpers are
> fresh-context agents launched through the Agent tool: `general-purpose`, and
> `reviewer`, `risk-assessor` and `review-pr-runner` from `.claude/agents/`.
> Shell variables don't carry over between `Bash` calls, so write each value
> Setup records, such as `START` or the PR base, literally into later commands
> and task texts.

=== helpers
Launch helpers one at a time with the Agent tool, using the agent types below
and the task templates at the end of this file. A helper runs in the
background and its reply arrives as a notification: wait for it, and launch
nothing else meanwhile. Pass `model` set to the helper model (Setup), so every
helper runs on this session's model.

Every agent of the run thinks at max, except the review's discovery agents:
review-pr runs them as `reviewer` at its default, high, because they only read
code. Pass `effort: "max"` to every helper launch; `review-pr-runner`,
`risk-assessor` and the review's `review-prober` falsifiers set max in their
definitions.

| Helper | Agent type | Extra parameters |
|---|---|---|
| Analysis | `general-purpose` | `effort: "max"` |
| Check | `reviewer` | `effort: "max"` |
| Build | `general-purpose` | `effort: "max"` |
| Test review | `reviewer` | `effort: "max"` |
| Test gaps | `general-purpose` | `effort: "max"` |
| Risk | `risk-assessor` | none |
| Review | `review-pr-runner` | none |

=== setup-model
   model too: the family of this session's model, from the model ID in the
   system prompt (`claude-opus-5-5` gives `opus`).
=== setup-effort-check
=== helper-questions
- A helper can't ask the main session anything mid-task. One that needs a
  decision stops and gives the reason in its reply (`STOPPED`, or `ASK` or
  `BLOCK` from the analysis); handle that reply as usual. Never settle a
  design question from a reply yourself - new logic, another approach -
  since the main session hasn't read the code. Only the analysis and the
  check decide a design.
