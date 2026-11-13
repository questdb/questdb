=== frontmatter
---
name: review-pr
description: {{description}}
allowed-tools: bash read subagent
metadata:
  argument-hint: "{{argument_hint}}"
---
=== harness-note
> Harness note: this skill runs inside **pi**. Use `bash` for read-only `gh`, `git`,
> `rg`, `grep`, and `find` commands, `read` for files, and fresh-context `review-reader`,
> `review-prober` and `risk-assessor` agents through `subagent(...)`. Do not edit
> files or push. Build and run tests only in the review's scratch worktrees (Steps
> 1 and 3b), never in the user's working tree. When this skill is run as
> `/skill:review-pr <args>`, the `<args>` are appended as a `User:` message;
> treat that text as `$ARGUMENTS`.
=== model_example
<provider>/<model>, thinking <level>
=== model-id
Take the model from pi, never from memory: models misstate their own name and version.

```bash
printf '%s/%s, thinking %s\n' "$PI_PROVIDER" "$PI_MODEL" "$PI_REASONING_LEVEL"
```

If it prints no model, write `model unknown`.
=== spawning
Step 3 uses fresh-context `review-reader` agents (`.pi/agents/review-reader.md`) through
`subagent(...)`, one task per role. Discovery reads code and runs nothing: a `review-reader`
uses its shell only to read and search, thinks at high, and keeps the 30-minute default
timeout. Step 3b uses fresh-context
`review-prober` agents (`.pi/agents/review-prober.md`), one per atomic candidate:
read-only reviewers with a shell that run the probes the evidence rules ask for. Give
each falsifier `timeoutMs: 3600000` and, in its task, a budget of about 30 minutes: a
probe that needs longer reports what it has and marks the rest `unknown`. Step 3c uses
fresh-context `risk-assessor` agents (`.pi/agents/risk-assessor.md`), one per finding
it rates, each with `timeoutMs: 3600000`. Launch all of them as foreground calls (`async: false`; a parallel fanout
still runs them at once).
