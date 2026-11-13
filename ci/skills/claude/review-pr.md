=== frontmatter
---
name: review-pr
description: {{description}}
argument-hint: "{{argument_hint}}"
allowed-tools: Bash, Read, Grep, Glob, Agent
---
=== harness-note
> Harness note: this skill runs inside **Claude Code**. Use `Bash` for read-only `gh`
> and Git queries and for `rg` and `fd` searches, `Read` for files, and fresh-context
> `review-reader`, `review-prober` and `risk-assessor` agents (`.claude/agents/`) through
> the Agent tool. Where a build has the `Grep` and `Glob` tools, they can replace `rg`
> and `fd`; without `fd`, use `find`. Do not edit files or push. Build and run tests
> only in the review's scratch worktrees (Steps 1 and 3b), never in the user's
> working tree. Shell variables don't carry over between `Bash` calls, so write
> each value a step records, such as `$BASE`, `$WT` or `$TMP`, literally into
> later commands.
=== model_example
<model ID>
=== model-id
Copy the model ID from your system prompt; never write it from memory: models misstate their own name and version. Without such a line, write `model unknown`.
=== spawning
Step 3 uses fresh-context `review-reader` agents (`.claude/agents/review-reader.md`) through the
Agent tool, one task per role. Discovery reads code and runs nothing: `review-reader` uses
its shell only to read and search, and thinks at high effort. Step 3b uses
fresh-context `review-prober` agents (`.claude/agents/review-prober.md`), one per
atomic candidate: read-only reviewers with a shell that run the probes the evidence
rules ask for. Give each falsifier, in its task, a budget of about 30 minutes: a probe
that needs longer reports what it has and marks the rest `unknown`. Step 3c uses
fresh-context `risk-assessor` agents (`.claude/agents/risk-assessor.md`), one per
finding it rates. Launch each step's agents in parallel and wait for all of them
before the step goes on.
