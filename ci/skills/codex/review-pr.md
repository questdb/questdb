=== frontmatter
---
name: review-pr
description: {{description}}
---
=== harness-note
> Harness note: this skill runs inside **Codex**. Use shell tools for read-only `gh`,
> Git, `rg` and `rg --files` queries (`rg --files` replaces `fd` wherever this skill
> names it), local file reads for source inspection, and
> fresh-context Codex subagents of the `reviewer`, `review-prober` and `risk-assessor`
> agent types (`.codex/agents/`). Do not edit files or push. Build and run tests only in the review's scratch
> worktrees (Steps 1 and 3b), never in the user's working tree. Shell variables
> don't carry over between shell calls, so write each value a step records, such
> as `$BASE`, `$WT` or `$TMP`, literally into later commands.
=== model_example
<model>, effort <effort>
=== model-id
Take the model from this session's rollout file, never from memory: models misstate their own name and version.

```bash
f=$(find "${CODEX_HOME:-$HOME/.codex}/sessions" -name "rollout-*-$CODEX_THREAD_ID.jsonl" | head -1)
grep '"type":"turn_context"' "$f" | tail -1 | jq -r '.payload | "\(.model), effort \(.effort)"'
```

If it prints no model, write `model unknown`.
=== spawning
Steps 3, 3b and 3c use fresh-context Codex subagents: `spawn_agent` with
`fork_turns: "none"`, so a subagent sees only its task, and `agent_type` set to its
role, whose definition is in `.codex/agents/<role>.toml`. Step 3 uses `reviewer`
agents, one task per role, at `reasoning_effort: "high"`. Discovery reads code and
runs nothing: a `reviewer` uses the shell only to read and search. Step 3b uses
`review-prober` agents, one per atomic candidate: read-only reviewers with a shell
that run the probes the evidence rules ask for. Give each falsifier, in its task, a
budget of about 30 minutes: a probe that needs longer reports what it has and marks
the rest `unknown`. Step 3c uses `risk-assessor` agents, one per finding it rates.
The `review-prober` and `risk-assessor` definitions set `xhigh` effort, which
overrides any `reasoning_effort` a launch passes. Launch each step's agents in
parallel, as many at once as the session's concurrency slots allow, and wait for all
of them with `wait_agent` before the step goes on.
