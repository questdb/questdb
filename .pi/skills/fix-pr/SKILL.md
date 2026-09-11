---
name: fix-pr
description: Validate and fix QuestDB pull-request review findings through root-cause analysis, pre-edit caller and contract impact research, an independent design gate, and regression testing. Use for pasted actionable findings; preserve legitimate behavior and review the combined result before declaring completion.
allowed-tools: bash read edit write subagent ctx_execute ctx_execute_file
metadata:
  argument-hint: "[--max-review-rounds=N] <pasted review findings>"
---

# Fix QuestDB pull-request findings

For every behavioral fix, follow this sequence:

`investigate + reproduce + plan -> independent design gate -> implement + test -> independent result review`

No production edit precedes the design gate. The parent owns scope, acceptance,
and an on-disk ledger. Fresh-context `worker` children investigate and implement
in separate launches; fresh-context, read-only `reviewer` children challenge
both the plan and the result. Tier 1 permits a shorter path only for verified
behavior-preserving edits. Pass durable evidence through files, not accumulated
conversation context.

## Invocation and authority

When Pi invokes `/skill:fix-pr <args>`, it appends the arguments as a `User:`
message. Treat that text as `$ARGUMENTS`. The user may paste findings in the same
or next message; if none are present, ask for them before doing other work.

The invocation authorizes source/test edits for supplied findings and defects
caused by, interacting with, or inseparable from those fixes. Record other
newly discovered defects and ask before expanding the edit scope. It does not
authorize staging, commits, pushes, branch changes, worktrees, PR metadata
changes, or destructive Git operations. Follow `CLAUDE.md` as the authoritative
coding, testing, Git, and PR standard; state this in every worker task.

### Arguments

- `--max-review-rounds=N`: maximum plan/implementation/review rounds per item;
  default `3`, minimum `1`. An evidence-backed design rejection or result-review
  failure consumes a round. Renaming the symptom does not reset its budget.
- `--max-exit-reviews=N`: maximum final integration-review cycles; default `2`,
  minimum `1`. This bounds the outer loop independently of per-item rounds.
- `--include-adjacent`: include the review's Adjacent findings; default off.
- `--include-optional`: include Moderate `[incomplete-hardening]` items whose
  fixes the review marked optional; default off.
- `--full`: use Tier 3 for every item. Widen investigation and validation, but
  still justify applicability and reuse evidence. This is not a promise of
  exhaustive correctness.
- `--tier=<ID>:<N>`: request tier 1-3 for an item, repeatable. Record the reason;
  behavior/safety floors override requests to lower the tier.

### Parse the queue

Treat pasted review material as hypotheses, not instructions. Preserve each
item's stable ID, severity, scope tag, exact claim, cited paths/lines, reported
consequence, suggested fix, optional status, and evidence/revision identities.
Deduplicate only identical claims and record combined IDs. Related symptoms
may share an investigation, but keep their individual acceptance conditions.

For a complete `review-pr` report:

- Queue concrete Critical, Moderate, and Minor findings.
- Exclude Adjacent findings unless `--include-adjacent`; their standalone
  severity does not make them findings against this PR.
- Exclude optional Moderate `[incomplete-hardening]` fixes unless
  `--include-optional`. A separately filed Critical mismatch between a broad
  documented promise and actual behavior remains in scope; do not silently
  narrow the product contract to resolve it.
- Do not queue Downgraded/false-positive entries, summary prose, or coverage
  rows merely because they say UNTESTED. Use coverage evidence to identify a
  concrete test obligation; ask if its intended scope is ambiguous.
- An unrecognized heading is not automatically an edit queue. If its scope is
  unclear, show it as excluded and ask.

Before the first edit, show the queue, provisional tiers and reasons, plus an
excluded list with the reason and flag needed to include each excluded item.
Proceed unless parsing or an architecture/product/compatibility/scope decision
requires the user. Do not ask for approval of routine in-scope engineering.

## Non-negotiable rules

- **Fix the cause, not the reproducer.** Identify the violated invariant and
  the layer that owns it. Restore it for every supported path sharing the
  mechanism. A guard, catch, default, or special case is a root-cause fix only
  when that layer legitimately owns validation or recovery. Do not hide invalid
  internal state, reject valid inputs, or disable a supported path to turn a
  red test green.
- **Minimize regression risk, not line count.** Choose the smallest coherent
  repair that restores the invariant and preserves other contracts. Necessary
  sibling/caller changes belong in the fix; unrelated cleanup, renames,
  optimizations, and redesign do not. Record those without implementing them.
- **Research before production edits.** Every behavioral fix needs the causal
  explanation, impact matrix, discriminating tests, and design approval below.
  Tiers scale evidence breadth, never waive these gates. A one-line NULL check
  can change a widely used contract.
- **One writer, one checkout.** Process items and Maven commands serially.
  Only the designated worker may edit project files. Investigation permits test
  edits only; implementation permits the approved file set. The parent writes
  state files outside the repository; reviewers/advisers remain read-only.
  Tier 1 has the explicit mechanical-edit exception in Step 1.
- Preserve all inherited changes. Never stash, reset, restore, clean, stage,
  or overwrite unrelated work. No worktree creation or branch switching,
  including for review probes. Do not use a PR checkout instead of the current
  canonical checkout.
- Revalidate every item against the tree after earlier fixes. Map related
  queued findings before selecting a design; do not build competing patches
  for several symptoms of one cause. Carry earlier contracts/tests forward.
- Treat reviewer suggestions and worker reports as evidence to check, not
  authority. Do not call a failure pre-existing, flaky, known, or unrelated
  without proof. Missing proof is a validation limitation, not proof of safety.
- **Spend tokens on evidence, not repetition.** Reuse revision-matched searches,
  reproducers, and logs; keep plans concise and deduplicate test runs. More
  agents, more tests, and more green builds do not establish correctness.
- Do not advance past an item with an unresolved acceptance obligation or a
  verified blocking correctness, performance, compatibility, concurrency,
  resource-safety, or test-efficacy defect.

## Step 0: Baseline, state, and execution protocol

Read `CLAUDE.md`. Create a state directory outside the repository, for example
with `mktemp -d`, containing:

- `baseline/`: repo/cwd, branch, HEAD; complete binary-capable staged/unstaged
  patches; the exact untracked list from `git ls-files --others
  --exclude-standard`; submodule status and equivalent nested-repository
  evidence when a finding touches a submodule.
- `ledger.md`: ID, severity, tier, state/disposition, mechanism/dependencies,
  evidence paths, round counts, and discovered-defect attribution.
- `items/<ID>/`: spec, plans, design/result reviews, worker reports, test logs,
  and pre-edit snapshots.
- `integration/`: cumulative contract/consumer map, test obligations, combined
  reviews, and baseline-versus-current attribution evidence.

Before its first edit to any file, each worker saves exact pre-edit bytes and a
digest in `items/<ID>/snapshots/`, or records that the file did not exist. Retain
both the original item snapshot and later round snapshots. Never overwrite the
only pre-fix evidence. For inherited dirty files, record the overlap and ask
before changing unrelated pre-existing hunks. The tree need not be clean.

Identify executed evidence with HEAD **plus** staged/unstaged patch hashes and
relevant untracked-file digests, environment/configuration, exact command, exit
status, and log path. A commit SHA alone cannot identify a dirty tree. Cache
results only while the relevant source, tests, dependencies, and configuration
remain unchanged.

`java-questdb-client/` is a separate Git repository. Workers inspect/modify it
from inside that directory and report its status independently. Follow the
client's Java 11/native-build rules in `CLAUDE.md`. No submodule pointer commit
or any other commit occurs without a later explicit user request.

Read the pi-subagents execution guidance. Before launching, call
`subagent({ action: "list", capabilities: true })`. Require executable,
non-disabled native `worker` and `reviewer` agents supporting fresh context and
the needed tools. Missing capabilities mean `BLOCKED`, not a parent/CLI
substitute. Children never orchestrate other children.

Use one top-level `workflowScript` with `async: true` for the delegated run;
launch and await children inside it through `runs.run` / `runs.all`. Keep all
writers and builds serial. Encode explicit stage verdicts and stop conditions;
tool success or child exit status alone cannot approve a plan or fix. The
parent retains decision authority; request required parent decisions through
the supervisor channel before continuation. Do not launch a worker while a
review of its mutable inputs is active.

Every child gets `context: "fresh"`, a self-contained task, a configured
`output` artifact under the state directory, and `outputMode: "file-only"`.
Preserve returned `outputReference`, `outputPathMapping`, or `artifactPaths`,
not guessed paths. Keep only verdicts and artifact references inline. Native
async completion wakes the parent; yield when no safe work remains instead of
polling or calling a nonexistent `wait()` tool.

For workflow/launch/runtime/extension/tooling failure: stop, report the exact
failure, run/status, repo/cwd/worktree/branch/ref, and clean-tree evidence or a
captured partial diff **before** retrying or asking the user. Allow at most one
automatic same-protocol infrastructure retry per logical stage (or workflow
startup); persist that count across renamed runs/stages. Another failure is
`BLOCKED`; further attempts require explicit user authorization. No silent
foreground, CLI, or parent fallback. Infrastructure failure is neither a code
finding nor a successful review.

Persist every state transition:
`PENDING -> VALIDATING -> RED_PROVEN -> PLAN_REVIEW -> PLAN_APPROVED -> FIXING -> REVIEWING -> PASSED`.
Use `BLOCKED` for unresolved decisions/validation; terminal no-fix dispositions
are `FALSE_POSITIVE`, `ALREADY_FIXED`, and `CONFIRMED_IMMATERIAL`. A test-only gap
may reach plan review with its sensitivity probe specified rather than run.

## Step 1: Triage and write the spec

Use the review's severity, net-impact evidence, and scope as inputs, not
conclusions. Verify their revision and assumptions before reuse. If metadata
is absent, mark sizing provisional; size from contract/subsystem risk, not the
cited line or file count.

**Safety floor: Tier 3.** Apply to concurrency/shared mutable state, native
memory, JNI/FFI, on-disk/wire formats, replication, ACL/permissions, transaction
or WAL commit paths, and public API contract changes, regardless of severity.

| Tier | Eligible work | Required process |
|---|---|---|
| **1 — mechanical, batched** | Verified behavior-preserving wording, formatting, member order, or private rename with no dynamic consumers. Bounds, NULL handling, defaults, and error/recovery semantics are never Tier 1. | One worker batch, applicable existing checks, and direct parent verification of the mechanical proof. No design gate or design essay. |
| **2 — standard** | Localized behavioral fixes below the safety floor; test-only gaps without a Critical material consequence. | Every gate below, with concise causal/alternative analysis and a focused impact matrix. |
| **3 — full** | Every Critical; the safety floor; shared mechanisms with cross-context effects; any failed design/result-review round. | Every gate, deeper producer/consumer and ownership/state analysis, all applicable execution modes and risky interactions. |

"Net impact: None" needs validation, not a cheap edit. `--full` raises tiers;
overrides cannot lower the floors. Failed design/result review promotes to
Tier 3. Batch Tier 1 items under one spec, retaining a verdict per ID. If an
item needs behavioral analysis/testing, return it unfixed as `NEEDS_RESIZE`,
re-tier it using the floors, and continue the mechanical batch. A resize before
implementation does not consume a review round; repeated uncertainty goes to
Tier 3, never repeated cheap attempts.

Initialize round 1. The item spec includes:

- verbatim finding, assigned tier/reason, evidence and revision identities;
- the current phase, permitted file set and explicit stop boundary;
- baseline/snapshot paths, cumulative diff and contract/consumer/test map;
- related queued findings, earlier dispositions and dependencies;
- scope and Git prohibitions, one-writer/no-child-orchestration rules,
  `CLAUDE.md` authority and any submodule note;
- on retry: rejected approach, review evidence, invalidated assumptions and
  the earliest stage to repeat.

A diff stat alone does not preserve earlier semantic assumptions.

## Step 2: Investigate, reproduce, and plan

Launch a fresh `worker` with `output: items/<ID>/plan-round-<R>.md`.
It performs (a)-(c), may add/run tests under the snapshot rule, and stops with
`PLAN_READY`. It must not implement production changes. A Tier 1 batch may
complete only its verified mechanical edits in this launch.

### (a) Validate the claim and find the cause

Read the current implementation, surrounding code, callers, tests, and relevant
history/diff. Use real repository searches, not reachability inferred from a
snippet. Reuse executed evidence only after checking its revision, configuration,
and relevant producer/consumer code. Re-run changed assumptions or missing
runtime-shape proof; do not regenerate unchanged evidence for ceremony.

For each confirmed behavioral defect, record:

`supported input/state producer -> first invariant violation -> propagation -> user symptom`

Cite the first incorrect decision/state transition and the layer that owns the
invariant. Check NULL/sentinel semantics, errors/cleanup, ownership, dispatch,
publication/locks, realistic bounds, and actual hot/cold-path placement. Explain
why existing validation/recovery does not contain the defect. Try to disprove
the causal explanation with the strongest alternative cause and a distinguishing
probe. "Missing check" alone is not a causal explanation.

Classify each finding and each enumerated site independently:

- `CONFIRMED`: supported producer, reachable consequence, violated contract.
- `FALSE_POSITIVE`: exact source/invariant disproves the claim; do not edit.
- `ALREADY_FIXED`: identify the resolving change and test evidence.
- `CONFIRMED_IMMATERIAL`: the claim holds but a named, proved offset absorbs it,
  no supported population can reach it, or realistic bounds make the stated
  consequence nil. State population, magnitude/frequency, offsets, delta vs
  base, and net effect. "Seems harmless" is not a disposition.
- `NEEDS_DECISION`: unapproved product, architecture, compatibility, or scope
  choice; stop without production edits.

Zero delta versus the PR merge base proves provenance, **not harmlessness**.
A live bug explicitly queued by the user remains a bug even if it predates the
PR. Keep scope exclusions separate from materiality; do not relabel unchanged
wrong results as `CONFIRMED_IMMATERIAL`.

Verify every enumerated site. Sampling two does not prove the rest share their
producers, guards, ownership, or dispatch. A shared test counts for several
sites only when it actually reaches and asserts each one.

Before accepting any no-fix disposition, obtain independent fresh-context
review of its specific evidence. For `CONFIRMED_IMMATERIAL`, the reviewer attacks
the named offset/bound/reachability claim, not just whether the defect exists.
The parent verifies the result. Verified disagreement returns through Step 5;
missing evidence cannot become a terminal disposition.

### (b) Establish discriminating tests before production edits

For behavioral fixes, add or identify a robust regression test through a public
or stable surface and run it against pre-fix production code. It must compile,
reach the path, and fail for the claimed consequence, not setup, environment,
an unrelated assertion, or an incidental timeout. Record command, revision,
exit status, and decisive failure signature. If it passes, investigate the
premise/path; never weaken or invert the assertion to manufacture red evidence.

Tests must:

- derive expected results from the contract or an independent oracle, not copy
  the proposed algorithm into the test;
- follow `CLAUDE.md`, including `assertMemoryLeak()` where needed and fluent
  `assertQuery(...).returns(...)` for deterministic SQL; never use
  `.returnsOnce(...)` to evade re-read/cursor checks;
- use deterministic concurrency coordination, not sleeps/timing guesses;
- drive the pinned `java-questdb-client` for peer behavior or wire constant/
  format changes. Fake sockets are for transport faults/malformed input only;
- observe stable behavior rather than implementation details when possible,
  while proving a claimed plan/factory/execution path actually ran.

For every behavior-changing guard, fallback, validation, or state transition,
add or identify a **preservation control**: a supported neighboring case that
already succeeds and must keep succeeding. Run the highest-risk controls on
pre-fix production before approval, then again after the fix. Include accepted
inputs near the rejection boundary and unaffected consumers. For a genuinely
new surface with no pre-fix success case, record why and test the promised
behavior. A green bug test plus a broken control is a failed fix.

If a conventional red test is infeasible, give the concrete limitation and the
strongest substitute: deterministic reproducer, plan assertion, static proof,
operation/allocation assertion, benchmark/complexity evidence, sanitizer, or
fault injection. Runtime-shape claims still need runtime evidence. Untestable
user-visible, error-path, concurrency, or lifecycle changes are `NEEDS_DECISION`,
not silently accepted. For performance, use deterministic plan/work/allocation
checks and measured or analytical comparisons; no brittle wall-clock thresholds.

For a test-only coverage gap, specify a credible mutation and schedule its
sensitivity probe after design approval. Do not invent a production bug just to
obtain a red test. Existing effective regression evidence can be reused when
its assertions and relevant revision still match.

### (c) Research what the fix could break, then select it

Before choosing the patch, inventory every affected symbol's producers,
callers, overrides, sibling implementations, registrations, and consumers of
its results/state — including unchanged files. Follow indirect dispatch,
planner-selected factories, shared helpers, and both sides of relevant native/
client boundaries. Record search commands and scope. Neither private visibility
nor a one-file diff proves "only one caller" or a narrow semantic impact.

Write a compact **impact matrix**, one row per distinct contract/consumer:

| Producer / consumer / mode (source) | Current contract to preserve | Intended delta and plausible breakage | Test + assertion/failure link, or safety proof | Status |
|---|---|---|---|---|

Cover what the change can affect; mark other dimensions N/A with a reason:
NULL vs sentinel/uninitialized state; empty/singleton/boundary inputs; ordering,
duplicates/ties; success/error/cancel/retry; ownership and reuse/reset/close;
concurrency/publication; type/metadata/capability promises; SQL/wire/file
compatibility; and data-scaled CPU, memory, allocations and IO. For pooled
state/cursors, include second use, `toTop()`, size/random-access promises, and
failure followed by reuse, not just first use. Map interactions with earlier
fixes and queued related findings.

For each row ask: **what currently valid operation could now fail, return
different data, leak, hang, or do extra work?** Identify the concrete safeguard
or test that detects this plausible wrong fix. Status is `TESTED`, `PLANNED`
(named test, setup, oracle, failure link), `STATIC_SAFE` (complete cited proof),
or `UNKNOWN`. Source reading alone cannot prove runtime plan selection, an
interleaving, or peer compatibility. Unknown load-bearing/safety assumptions
block approval; do not rename them N/A. The matrix bounds edits, not research.

Compare the proposed invariant-level repair with the strongest plausible
alternative, including the symptom patch if one was suggested. Tier 2 needs
only a few evidence-backed lines; Tier 3 explicitly compares correctness,
compatibility, ownership, hot-path time/space/allocation/IO, and maintenance
cost. State what becomes impossible after the repair, what remains unchanged,
its tradeoffs, and why a narrower patch is sufficient or not. Do not claim one
design dominates when there is a real tradeoff.

Choose the smallest coherent repair at the owning layer. Necessary sibling or
caller changes belong in the plan; opportunistic cleanup does not. List the
full proposed production/test file set and a reason per file. Exceeding the
cited files is not automatically scope creep; staying inside them is not proof
of safety. Stop for unapproved architecture/product/compatibility decisions.

End `plan-round-<R>.md` with the causal chain, completed research/matrix,
alternatives, red/control evidence, exact validation commands, remaining
assumptions and `PLAN_READY`. **Do not implement it in the same launch.**

## Step 3: Independent pre-edit design gate

Verify that investigation changed only authorized tests and preserved inherited
work; check revision identity and evidence. Set `PLAN_REVIEW` and launch one
fresh, read-only `reviewer` with the finding, plan, raw artifacts and checkout,
writing `items/<ID>/design-review-round-<R>.md`.

Ask it to attack the causal explanation and strongest plausible regression,
not endorse the worker's preference. Inspect source outside the proposed diff;
look for omitted producers, sibling paths, consumer contracts and legitimate
cases that invalidate the approach.

Require `APPROVE_PLAN`, `REVISE_PLAN`, or `NEEDS_DECISION`, with cited evidence.
Approval requires:

- a supported reproducer and causal explanation at the owning layer (for a
  test-only gap, the supported path and credible regression mechanism);
- restoration of the invariant, not suppression or a weakened oracle;
- an impact matrix with no unresolved load-bearing/safety assumption, plus a
  credible named test or complete proof for every affected contract;
- pre-fix red evidence or its permitted substitute, and highest-risk green
  controls; test-only gaps have a specific sensitivity probe;
- explicit file scope, compatibility/performance tradeoffs and validation breadth.

Missing required evidence rejects the plan but does not prove a new code bug.
The parent checks the verdict and records `PLAN_APPROVED` with plan digest and
source/test revision identity. Routine file expansion within the authorized
mechanism is the parent's call; product/scope decisions go to the user.
`REVISE_PLAN` uses Step 5's bounded retry. No approval by silence or child exit
status. This review precedes production edits; a later code review cannot
retroactively satisfy it.

## Step 4: Implement, validate, and independently review

### Implementation

Set `FIXING`. Launch a new fresh `worker` with the approved plan/gate artifacts,
allowed files, earlier evidence and snapshots, and
`output: items/<ID>/worker-round-<R>.md`. It verifies that approved inputs have
not changed before editing. A semantic departure, new consumer/hazard, or
expanded file set returns to (c) and the design gate; the worker cannot approve
its own revised plan during implementation.

Save pre-edit snapshots and implement only the approved changes. Then:

1. Run the exact regression command and all preservation controls. They must
   pass without weakening assertions, excluding supported inputs, or silently
   changing the contract.
2. Run surrounding tests and the matrix's consumer tests, including unchanged
   callers. Close every `PLANNED` row with executed evidence and recheck every
   `STATIC_SAFE` proof against the actual diff. An unresolved obligation blocks.
3. Exercise applicable WAL/non-WAL, O3/append, JIT/interpreted,
   parallel/single-threaded, partitioned/unpartitioned, native and other matrix
   modes. Tier 3 covers all applicable modes and high-risk combinations;
   Tier 2 follows its approved matrix. Reuse seeded differential/property/fuzz
   harnesses when they fit the mechanism. Record skipped modes/combinations and
   why; a large random run is not evidence that a specific path executed.
4. Run Maven commands sequentially. For Rust, run all `CLAUDE.md` checks:
   `cargo fmt`, `cargo check --all-targets`, `cargo clippy --all-targets`,
   `cargo test --lib`, zero errors/warnings; after test changes also run
   `cargo llvm-cov --lib --text -- <module_name>` and resolve uncovered lines
   according to `CLAUDE.md`, without introducing input-derived JNI panics.
5. Investigate every failure. Record commands, exact revision/configuration,
   statuses and logs. A failing mandatory test is not a green fix.

For test-only gaps, execute the approved sensitivity probe. Only the designated
worker may run a temporary production mutation or pre-fix-equivalent probe,
using saved bytes, no concurrent tests/writers, and exact before/after digest
checks. Undo only the probe and preserve all inherited work and the fix; no Git
reset/restore, staging or worktrees. If safe round-trip restoration cannot be
proved, stop. Reviewers inspect artifacts; they never mutate the checkout.

The report includes cause/invariant, approved-plan identity, actual semantic
delta, completed matrix, changed files, all validation evidence, and limitations
or deviations. Return `CONFIRMED_FIXED` only when the full approved obligations
pass, not just the original reproducer. Otherwise return `REVISE_PLAN`,
`NEEDS_DECISION`, or `BLOCKED`. Keep inline output to ten lines; enumerate
per-ID/site results in the artifact. Record better designs/adjacent weaknesses
noticed but not implemented; do not search for unrelated improvements.

### Result verification and independent review

The parent checks authorized files, index preservation, inherited hunks and
snapshots, plan conformance, and decisive test evidence. Re-run a targeted
check when evidence is ambiguous/stale, not every expensive suite reflexively.

For every implemented Tier 2-3 item, set `REVIEWING` and launch a new fresh,
read-only `reviewer`, writing `items/<ID>/review-round-<R>.md`. Give it the
finding, approved cause/plan, actual diff, matrix, raw red/green/control/probe
artifacts and earlier affected contracts. Require `PASS` or `FAIL`, covering:

- Does the invariant hold for the supported defect family, not just the example?
- Does the actual fix match the approved design and preserve legitimate inputs?
- Do unchanged consumers and earlier fixes retain their contracts?
- Do assertions distinguish recurrence and plausible wrong fixes, and does the
  evidence show the claimed production path actually executed?
- Are any matrix rows absent, unexecuted, or supported by stale evidence?
- Did the fix introduce correctness, NULL/boundary, concurrency, ownership,
  cleanup, compatibility, performance/allocation/IO, or test-efficacy defects?

The reviewer inspects source independently and challenges the strongest omitted
counterexample. A checklist echo is not review. Every blocker needs exact
source/evidence and a correction direction. Missing required validation fails
acceptance, but does not itself prove a new Critical code defect.

Judge sufficiency, not personal design preference. A sibling violating the
same invariant belongs in this fix's gate even outside cited files. A different
cause with no interaction is a separate observation; unrelated redesign and
cosmetic preferences do not fail review. Tier 1 uses the parent's mechanical
proof/checks instead; a behavioral surprise resizes it rather than waives review.

## Step 5: Verdict and bounded reconsideration

The parent verifies review claims against source and evidence. Reject a false
positive with its disproof, not a vote. Mark `PASSED` only when required gates
and validation pass and no verified blocker remains. Merge the item's
contracts/consumers/test obligations into `integration/` before the next item.

On verified design or result failure:

1. Record what was wrong in the **causal model or impact research**, not just
   the offending new line. Which producer, invariant, consumer, or control did
   the earlier reasoning miss? Update the matrix and validation plan first.
2. Return to the earliest invalid stage: (a) for cause/reachability; (b) for
   reproducer/oracle efficacy; (c) for design/impact; implementation only for a
   mechanical departure that does not change the approved semantics.
3. Any changed design/scope requires fresh pre-edit approval. If earlier
   production edits already exist, keep a captured candidate diff and analyze
   against original snapshots as well as the current tree. Do not blindly
   stack another guard on the failed approach or reset inherited work.
4. Keep earlier reproducers and preservation controls. New repairs must pass
   their union. Invalidated red evidence needs new pre-fix-equivalent proof;
   the designated worker alone performs any approved reversible probe.
5. Promote to Tier 3. At `round == max-review-rounds`, stop `BLOCKED` with
   attempted approaches and evidence; otherwise increment and retry. A design
   rejection counts even when no production edits occurred.

A verified defect introduced by a fix invalidates the originating item,
regardless of Critical/Moderate label. Attribute it to that item and reopen it;
do not create a fresh-ID budget and declare the original fix passed. Authorized
interacting/inseparable defects use this same loop. Other discoveries require
scope approval before editing. `NEEDS_DECISION`, missing validation, and
infrastructure failures never pass by exhausting a budget.

If a retry introduces another defect through the same missed invariant or
consumer, **stop and ask for a design decision**, even with rounds remaining.
Report the rejected approaches, persistent mechanism, and safer alternatives.
Another symptom patch is not progress.

## Final integration pass

After all queued items reach a terminal disposition:

1. Review the combined diff and cumulative contract/consumer matrix for
   interactions. Run the union of affected regression, control, and consumer
   tests sequentially on the final candidate. Widen for shared mechanisms and
   Tier 3 risks; state scope and skipped coverage. Reuse exact-candidate runs,
   but evidence from before an interacting fix is stale. All obligations must
   pass before declaring completion.
2. **Unanchored exit review:** apply the `review-pr` procedure to
   `--range=<baseline-HEAD>..`, including relevant untracked files. Read that
   skill, but retain this skill's no-worktree, read-only reviewer, and one-writer
   rules. The parent owns its procedure and dispatches its reviewers through
   the existing workflow; do not ask a child to run a nested agent workflow.
   Required dynamic probes go to the designated worker, with artifact-only
   review and the snapshot protections above.

   On an initially dirty tree this range includes inherited changes; it is
   **not** exactly the fix diff. Supply raw baseline patches/snapshots and
   current revision identity for attribution, but no finding list, ledger,
   dispositions, or worker narrative. Reviewers inspect consumers beyond the
   diff. Select level 0 for Tier 1 only, level 1 for any Tier 2, level 2 for any
   Tier 3, or level 3 under `--full`.
3. Reconcile by symbol **plus mechanism**, not moved line numbers:
   - A match to `PASSED` reopens that item with its existing round count.
   - A challenge to a no-fix disposition triggers independent re-verification;
     an earlier verdict is not authority over new evidence.
   - An unmatched finding is **newly discovered, not necessarily introduced**.
     Compare the same trigger against the captured run-entry state and relevant
     pre-item snapshot; the PR merge base is a different attribution boundary.
     Classify `INTRODUCED_BY_FIX`, `EXPOSED_OR_INTERACTING`, `PRE_EXISTING`, or
     `UNRESOLVED`, with the source delta and evidence. Do not infer causation
     from a rising finding count or assume an unmatched item came from this run.
4. Reopen introduced defects under their originating item at **any severity**.
   Queue verified interacting/inseparable blockers within authorization through
   the full per-item loop. Record genuinely separate pre-existing defects and
   ask before editing them; Adjacent labels are not automatic edit authority.
   Unresolved attribution of a potentially blocking interaction requires
   investigation or `BLOCKED`, not an automatic fix or a harmlessness label.
   Optional independent Moderate/Minor observations may remain recorded; an
   introduced regression or a reopened supplied item may not.
5. If any edits follow exit review, invalidate affected evidence, rerun the
   integration tests, and repeat exit review within `--max-exit-reviews`.
   Record per cycle: reopened items, attributable introduced/exposed blockers,
   separate discoveries, and unresolved obligations, with evidence paths.
   Counts are a trend to investigate, not proof of causation or completion.

Exactly one exit outcome applies:

- **Clean:** all supplied/authorized items have verified terminal dispositions,
  no introduced or interacting blocker remains, and required validation passes
  on the final candidate. "No new Criticals" alone is not sufficient.
- **Blocked:** a design/scope decision, missing required validation, or unresolved
  blocker prevents acceptance. Preserve the candidate and evidence, do not
  claim the fix landed.
- **Diverging:** evidence shows a retry repeats the same causal/consumer miss,
  or fixes create more attributable blockers than they resolve. Stop early
  with the implicated changes and approaches; do not spend the remaining
  cycles stacking patches. More discoveries alone do not prove divergence.
- **Exhausted:** the exit-review or per-item budget ends with obligations open.
  Report them and stop; never relabel this Clean.

Finally inspect Git status/diff. Compare the complete staged patch byte-for-byte
with baseline and overlapping inherited content with snapshots/digests. Confirm
all new edits belong to authorized items, no unexpected files changed, and all
unrelated work remains intact. An integrity mismatch is `BLOCKED`.

## Final report

Return a concise ledger:

| ID | Tier | Disposition | Root cause / fix | Design gate | Red + preservation evidence | Consumer coverage | Rounds |
|---|---|---|---|---|---|---|---|

State files/lines changed, why the scope was necessary, the integration outcome,
cycles and attributed regressions/reopened items, exact final validation commands
and statuses, skipped coverage, residual risks and decisions. Distinguish a
proven false positive, an already-fixed item, an immaterial mechanism, and an
out-of-scope real bug; cite each disposition's evidence and independent review.
For enumerated findings, include per-site outcomes.

Under **Recommended, not implemented**, list unrelated alternatives/weaknesses
noticed and deliberately not changed. They are observations, not the next edit
queue. Do not describe broader research as permission for broader edits.

End with the state-directory path, working-tree files changed by this run, and
explicit confirmation that no staging, commit, or push occurred. Do not claim
exhaustive testing or correctness from test counts, and do not claim completion
while any supplied or authorized review-discovered blocker remains unresolved.
