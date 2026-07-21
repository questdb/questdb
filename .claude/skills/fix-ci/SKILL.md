---
name: fix-ci
description: Check CI status, analyze test failures, auto-fix obvious issues or discuss with user
argument-hint: "[PR number or URL]"
allowed-tools: Bash, Read, Edit, Grep, Glob, Agent, AskUserQuestion
---

Analyze CI failures for PR `$ARGUMENTS` (or the current branch's PR if no argument given).

## Step 1: Discover build status

### 1a. Find the PR

```bash
# If $ARGUMENTS is a number or URL, use it directly.
# Otherwise detect from current branch:
gh pr view --json number,title,headRefName --jq '{number, title, headRefName}'
```

### 1b. Quick test summary (cheapest check, 1 API call)

Parse the buildId from any check URL first (see Step 1c), then hit the test summary endpoint:

```bash
curl -s "https://dev.azure.com/{org}/{project_guid}/_apis/test/resultsummarybybuild?buildId={buildId}&api-version=7.0-preview"
```

No auth needed. Returns aggregate counts instantly (~1KB):
```json
{
  "aggregatedResultsAnalysis": {
    "totalTests": 279518,
    "resultsByOutcome": {
      "Passed": {"count": 276837},
      "Failed": {"count": 24},
      "NotExecuted": {"count": 2657}
    },
    "runSummaryByOutcome": {
      "Failed": {"runsCount": 14},
      "Passed": {"runsCount": 23}
    }
  }
}
```

If `Failed.count == 0` and all checks passed — report success, stop.

### 1c. Fetch all CI checks

Use `gh pr checks` **text format** (tab-separated) — it reliably includes Azure URLs unlike the JSON `statusCheckRollup` where `detailsUrl` can be null.

```bash
gh pr checks $PR 2>&1
```

Output format (tab-separated):
```
CheckName\tstatus\tduration\tURL
```

Parse and classify each check:
- `pass` — succeeded
- `fail` — failed
- `pending` — still running

Group checks by source:
- **Azure Pipelines** — URL contains `dev.azure.com`. Extract buildId from first Azure URL. These are the test jobs.
- **GitHub Actions** — URL contains `github.com/actions`. These include `build` (compilation), `gitleaks` (secret scanning), `Danger` (PR linting). Failed GitHub Actions checks are relevant — report them.
- **Other** — `CodeRabbit` (no URL or review-only) — skip.

Extract buildId from the first Azure URL (all checks in one pipeline run share the same buildId).

### 1d. Report non-Azure failures

For each failed GitHub Actions check, fetch its details:

```bash
# Extract run_id and job_id from URL: https://github.com/{owner}/{repo}/actions/runs/{run_id}/job/{job_id}
gh run view {run_id} --json jobs --jq '.jobs[] | select(.conclusion == "failure") | {name, conclusion}'
```

Report these as Category E (non-test failures) in the final output. Common cases:
- **Danger** — PR convention issues (title format, description, labels). Show the comment: `gh pr view $PR --comments --jq '.comments[-1].body'`
- **build** — compilation failure. The job log contains the error.
- **gitleaks** — secret detected in diff.

### 1e. Triage

- **All passed + summary shows 0 failures** — report success, stop.
- **Some failed** — proceed to Step 2 with failed checks. Report summary: "24 test failures across 14 runs out of 279K tests."
- **All pending, none failed** — report "CI still running, no failures yet. Re-run this command later or use `/loop 2m /fix-ci $PR` to monitor."
- **Mix of pending + failed** — proceed with failed checks immediately. Don't wait for the full run.
- **Only non-Azure checks failed** (e.g., Danger, build) — report those directly, skip Steps 2-3.

## Step 2: Get failed test names (no auth, no log download)

Use the Azure Test Results microservice at `vstmr.dev.azure.com`. This is a different hostname from the build APIs and serves test result data publicly for public projects.

```bash
curl -s "https://vstmr.dev.azure.com/questdb/questdb/_apis/testresults/resultsbybuild?buildId={buildId}&publishContext=CI&outcomes=Failed&\$top=200&api-version=5.2-preview.1"
```

**No authentication or special headers needed.** Returns all failed test results (~1-5KB):

```json
[
  {
    "automatedTestName": "test[/sql/sample_by_fill.test]",
    "automatedTestStorage": "io.questdb.test.sqllogictest.SqlTest",
    "outcome": "Failed",
    "runId": 795795,
    "durationInMs": 1067.0,
    "id": 100001,
    "testCaseTitle": "test[/sql/sample_by_fill.test]"
  }
]
```

Key fields:
- `automatedTestName` / `testCaseTitle` — the test name
- `automatedTestStorage` — the test class (e.g., `io.questdb.test.sqllogictest.SqlTest`)
- `runId` — which CI job run this failure came from
- `outcome` — always "Failed" given the filter

**Deduplicate by test name** — the same test fails across multiple platforms (mac, windows, linux). Group by `automatedTestName`, collect `runId`s to know which platforms failed.

If this returns 0 results but Step 1b showed failures, fall back to Step 2b (log tail parsing). This can happen if the pipeline doesn't publish JUnit test results.

### 2a (optional). Enrich with error messages via PAT

The `vstmr` endpoint returns test names but NOT `errorMessage`, `stackTrace`, or `computerName`. If `AZURE_DEVOPS_PAT` is set, enrich each failed test with full details.

Step 2 gives us `runId` per failed test. Use the authenticated `test/runs/{runId}/results` endpoint to get error details:

```bash
# For each unique runId from Step 2:
curl -s -u ":$AZURE_DEVOPS_PAT" \
  -H "X-TFS-FedAuthRedirect: Suppress" \
  "https://dev.azure.com/questdb/questdb/_apis/test/runs/{runId}/results?outcomes=Failed&api-version=7.0"
```

This returns full details per failed test:
- `errorMessage` — the assertion failure or exception message
- `stackTrace` — full Java stack trace
- `automatedTestName`, `automatedTestStorage` — test identity
- `failingSince` — when this test started failing
- `failureType` — type of failure

With this data, skip directly to Step 4 (classification).

Note: `test/runs?buildId=...` (listing runs by build) requires `Build: Read` scope and returns 403 with `Test Management: Read` alone. But `test/runs/{runId}/results` works with just `Test Management: Read` — and we already have runIds from the unauthenticated Step 2.

**If no PAT is set**, suggest the user create one for richer failure data:

> To get error messages and stack traces without downloading logs, set `AZURE_DEVOPS_PAT`:
>
> 1. Go to https://dev.azure.com/questdb/_usersSettings/tokens
> 2. Click "New Token"
> 3. Set scope: **Test Management → Read** (the only scope needed)
> 4. Add to `~/.zshenv`: `export AZURE_DEVOPS_PAT=<token>`
>
> Without it, I can still see which tests failed but need to download log tails for error details.

Only suggest this once per session, and only if log tail parsing is actually needed (i.e., the test names alone aren't enough for classification).

### 2b. Fallback: log tail parsing (no auth)

Use this when Step 2 returns 0 results or when error messages are needed and no PAT is available.

#### Parse Azure URLs

Extract org, project GUID, buildId, jobId from each failed check's URL:
```
https://dev.azure.com/{org}/{project_guid}/_build/results?buildId={buildId}&view=logs&jobId={jobId}
```

All failed checks share the same buildId. Deduplicate: fetch the timeline only once per buildId.

### 2c. Fetch timeline

```bash
curl -s "https://dev.azure.com/{org}/{project_guid}/_apis/build/builds/{buildId}/timeline?api-version=7.0"
```

No authentication needed (public project).

Parse the JSON response. Records form a tree: Stage -> Job -> Task.
For each failed check's jobId:
1. Find all records where `type == "Task"` AND `parentId == jobId` AND `result == "failed"`
2. Extract `name` and `log.id` from each failed task

### 2d. Classify failed steps

- **"Run tests"** or **"Run tests with Coverage"** — test failure, proceed to log analysis
- **"Compile with Maven"** — compilation error, report to user directly
- Other steps — infrastructure failure (checkout, install, upload), report as-is

### 2e. Get log line counts

```bash
curl -s "https://dev.azure.com/{org}/{project_guid}/_apis/build/builds/{buildId}/logs?api-version=7.0"
```

Response: `{"value": [{"id": N, "lineCount": M, ...}, ...]}`. Extract lineCount for each failed step's logId.

## Step 3: Fetch and parse error summaries

### 3a. Download tail of each failed test log

Maven Surefire writes the error summary at the END of its output. Use the line-range API to fetch only the last ~500 lines:

```bash
curl -s "https://dev.azure.com/{org}/{project_guid}/_apis/build/builds/{buildId}/logs/{logId}?api-version=7.0&startLine={lineCount - 500}&endLine={lineCount}"
```

This fetches ~50KB instead of ~200MB. Save to a temp file for parsing.

### 3b. Parse the Surefire summary

Look for these patterns in the downloaded tail:

**Test error summary** (exceptions during test execution):
```
[ERROR] Errors:
[ERROR]   ClassName.testMethod:lineNum->...chain... >> ExceptionType message
```

**Test failure summary** (assertion mismatches):
```
[ERROR] Failures:
[ERROR]   ClassName.testMethod:lineNum expected:<X> but was:<Y>
```

**Totals line**:
```
[ERROR] Tests run: N, Failures: M, Errors: K, Skipped: L
```

**Compilation error** (different pattern entirely):
```
[ERROR] COMPILATION ERROR
[ERROR] /path/to/File.java:[line,col] error: ...
```

For each failed test, extract:
- Fully-qualified class name (e.g., `SqlParserTest`, `WindowFunctionTest`)
- Test method name
- Error type: assertion mismatch vs exception
- Error message / exception chain
- Line number in test source (from the `:lineNum` in the chain)

### 3c. Deduplicate across jobs

The same test might fail on multiple platforms (mac-griffin, windows-griffin, linux-griffin). Group failures by `ClassName.testMethod` — if the error message is the same across platforms, it's one logical failure. Note which platforms are affected.

## Step 4: Classify failures

### 4a. Get PR diff

```bash
gh pr diff $PR
```

Parse to understand:
- Which source files changed (production code vs test code)
- What functions/methods were modified
- What behavior changes the PR introduces

### 4b. Cross-reference each failure group with the PR diff

For each failed test group, determine the category:

**Category A — Auto-fixable** (ALL must hold):
1. The failure is an assertion mismatch (`Failures:` section, not `Errors:` section)
2. The PR modifies production code in the area the test covers (same package, same class, related function)
3. The PR already updated similar test assertions in the same or other test files (pattern exists in the diff)
4. The fix is mechanical: swap the expected value to match actual output
5. **Guard-removal gate**: if a test changed from "expected to fail" to "now succeeds" (e.g., error test that no longer errors), check the PR diff for removed guards (methods like `guardAgainst...`, `throw SqlException` blocks, early-return checks). If the PR removed a guard WITHOUT replacing the underlying functionality, the test likely exposes an unhandled code path — escalate to Category B/D. Only auto-fix if the PR replaced the guarded code with a new implementation that handles the case.

The heuristic: "Did the PR replace the functionality, or just remove the gate?" Replacing → auto-fix. Removing gate without replacement → discuss.

**Category B — Behavior precision**:
- The failure connects to PR changes (related classes/packages)
- But the PR did NOT already update similar tests, so intent is unclear
- Or the failure is an exception (not just a different value) that might indicate the test expected the old behavior to continue
- Or a guard was removed and the test now succeeds where it previously failed — need to verify the old code path is actually handled

**Category C — Potential regression**:
- The failing test is in a package/class NOT touched by the PR
- No clear connection between the test's subject and the PR's changes

**Category D — Potential incompleteness**:
- The test covers an edge case (NULL input, empty table, boundary value, special characters)
- The PR introduced new logic but the test suggests it doesn't handle this case
- Often: test expected a query to succeed, but new code throws an exception for this input

**Category E — Non-test failure**:
- Compilation error
- Infrastructure issue (timeout, OOM, disk full, network error)
- Flaky test (known flaky pattern, random ordering issue)

## Step 5: Act

### For Category A (auto-fix):

1. **Find the test source file:**
   - Convert class name to path: `io.questdb.test.griffin.FooTest` -> search in `core/src/test/java/`
   - Use Glob: `**/FooTest.java`

2. **Get the expected vs actual values:**
   - If the Surefire summary contains the full assertion diff (common with `assertEquals`): use directly
   - If the summary only has an exception message (common with `assertQueryNoLeakCheck`): need to search the full log
   - To search: use line-range chunks. First, find the `<<< FAILURE!` line for this test method:
     ```bash
     # Search in chunks of 50K lines from the end, looking for the test method name + FAILURE
     curl -s "...logs/{logId}?startLine={lineCount-50000}&endLine={lineCount}" -o /tmp/ci-chunk.txt
     grep -n "testMethodName" /tmp/ci-chunk.txt | grep -i "FAILURE\|ERROR\|expected\|but had"
     ```
   - Read +-100 lines around the match to get the full assertion diff

3. **Read the test method** in the source file. Find the assertion call and its expected value.

4. **Update the expected value** to match the actual output. Use the Edit tool.

5. **Report** what was changed:
   ```
   Auto-fixed: FooTest#testBar
   - Updated expected output: [brief description of what changed]
   - Reason: PR changed [behavior X], test expected old output
   - Platforms affected: mac-griffin, windows-griffin, linux-griffin
   ```

### For Categories B-E (discuss with user):

Present a structured report. Group by category, within each category group by similarity.

```markdown
## CI Failures: PR #NNN — [PR title]

Analyzed N failed jobs across M platforms.

### Auto-fixed (if any)
- `FooTest#testBar`: updated expected output — [description]

### Needs Discussion

#### Potential Regression (Category C)
Tests in areas NOT touched by this PR:
- `BarTest#testQux`: NullPointerException at SomeClass.java:42
  Platforms: linux-griffin, mac-griffin
  [Stack trace summary]

#### Potential Incompleteness (Category D)
New logic may not handle these cases:
- `WindowFunctionTest#testWindowAsArg`: SqlException "Window function is not allowed in context of aggregation"
  The PR added [feature X] but these tests show queries that combine window functions with aggregation.
  Platforms: all

#### Behavior Precision (Category B)
Connected to PR changes but need review:
- `SqlParserTest#testWindowFuncOrder`: expected query model differs from actual
  The PR changed [parser behavior X]; this test may need updating or may reveal unintended side effect.

#### Non-test Failures (Category E)
- Job `windows-cairo-2`: "Compile with Maven" step failed — compilation error in FooBar.java:123
```

After presenting, ask the user how to proceed with each group.

## Azure API Reference

### Test Results (vstmr.dev.azure.com) — no auth, no special headers

The test results microservice lives on a separate hostname. No authentication or special headers needed.

| Endpoint | Returns |
|---|---|
| `.../resultsbybuild?buildId={id}&publishContext=CI&outcomes=Failed&$top=200&api-version=5.2-preview.1` | Array of failed test results: `automatedTestName`, `automatedTestStorage`, `outcome`, `runId`, `durationInMs` |
| `.../resultdetailsbybuild?buildId={id}&publishContext=CI&groupBy=TestRun&$filter=Outcome eq Failed&shouldIncludeResults=true&queryRunSummaryForInProgress=false&api-version=5.2-preview.1` | Failed results grouped by test run, with counts per outcome |

Base: `https://vstmr.dev.azure.com/questdb/questdb/_apis/testresults`

### Test Result Details (dev.azure.com) — PAT with `Test Management: Read`

| Endpoint | Returns |
|---|---|
| `https://dev.azure.com/questdb/questdb/_apis/test/runs/{runId}/results?outcomes=Failed&api-version=7.0` | Full details: `automatedTestName`, `errorMessage`, `stackTrace`, `failingSince`, `failureType` |

Auth: `curl -u ":$AZURE_DEVOPS_PAT"`. Note: `test/runs?buildId=...` (listing runs) needs `Build: Read` scope, but `test/runs/{runId}/results` works with just `Test Management: Read` since we get runIds from the unauthenticated vstmr call.

### Test Summary (dev.azure.com) — no auth

| Endpoint | Returns |
|---|---|
| `https://dev.azure.com/questdb/{project_guid}/_apis/test/resultsummarybybuild?buildId={id}&api-version=7.0-preview` | Aggregate counts: total, passed, failed, not executed |

### Build APIs (dev.azure.com) — no auth

Base: `https://dev.azure.com/questdb/{project_guid}/_apis/build`

The project GUID is embedded in check URLs. Parse it from there rather than hardcoding.

| Endpoint | Returns |
|---|---|
| `/builds/{buildId}/timeline?api-version=7.0` | `{records: [{id, parentId, type, name, result, state, log: {id}, order}]}` |
| `/builds/{buildId}/logs?api-version=7.0` | `{value: [{id, lineCount, createdOn}]}` |
| `/builds/{buildId}/logs/{logId}?api-version=7.0&startLine=N&endLine=M` | Plain text, lines N through M |

### Timeline record types
- `Stage` — pipeline stage (parent of Jobs)
- `Job` — a CI job (parent of Tasks), maps to a GitHub check
- `Task` — a step within a job, has `log.id` for log download

### Status mapping
- `result="succeeded"` → success
- `result="failed"` → failure
- `result="skipped"` → skipped
- `result="canceled"` or `"cancelled"` → cancelled
- `state="completed"` with no result → success
- Otherwise → pending
