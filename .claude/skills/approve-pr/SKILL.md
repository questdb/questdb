---
name: approve-pr
description: Approve the pull request for the currently checked-out branch. Posts the review from this conversation as an approving review and adds the "QUEUED FOR MERGE" label. Use after review-pr when you decide to approve.
argument-hint: "[optional PR number or URL to override auto-detection]"
disable-model-invocation: true
allowed-tools: Bash, Read, Write
---

# Approve a QuestDB pull request

Approve the pull request for the branch that is currently checked out. This skill
is meant to be run **right after `review-pr`** in the same conversation: it takes
the review you just produced and posts it as an approving review. Do not
re-review here — post the existing review.

Actions performed, in order:
1. Post the review as the PR's review body (this is the "PR comment").
2. Approve the PR.
3. Add the `QUEUED FOR MERGE` label.
4. Tell the user which issue drafts need attention: the Deferred findings,
   crash drafts and security drafts. File none unless the user says so.

> Harness note: this skill runs inside **Claude Code**. Shell variables don't
> carry over between `Bash` calls, so once Step 1 resolves the PR number, write
> it literally into every later command and file path.

## Step 0: Confirm the review actually clears the bar

Only approve if the most recent `review-pr` report in this conversation ran at
level 3, as its opening line states, and has a verdict of **approve** or
**approve with comments** with ZERO open Blocking findings (per the review-pr
correctness gate). Deferred findings don't block: the PR ships with them, and
Step 4 lists them for the user. If the review ran at a lower level, do NOT approve
— tell the user to run `/review-pr --level=3` first. If a Blocking finding is
still open, do NOT approve — tell the user the PR does not meet the bar and
suggest `/reject-pr`.

- If no review report exists in this conversation, STOP and tell the user to run
  `/review-pr` first — do not approve unreviewed code.
- Post the review verbatim; do not shorten or rewrite it. The exceptions are
  what stays out of public view: leave out the Deferred section, each Adjacent
  draft the hostile-input rule covers, each security-sensitive draft
  (authentication or access-control bypass, data exposed across users, memory
  corruption), and any never-run line the coverage section lists for a failure
  the hostile-input rule covers. Tell the user what you left out.

## Step 1: Detect the PR and match the reviewed revision

Auto-detect the PR from the checked-out branch. An explicit PR number/URL in the
arguments overrides detection. Either way, resolve it to a PR number.

```bash
ARG='$ARGUMENTS'
PR=$(gh pr view ${ARG:+"$ARG"} --json number -q .number 2>/dev/null)
if [ -z "$PR" ]; then
  echo "No PR found for the current branch. Run 'gh pr checkout <n>' or pass a PR number."; exit 1
fi
gh pr view "$PR" --json number,title,author,headRefName,headRefOid,url,state,labels
echo "Approving PR #$PR"
```

The review's opening line names the revision it reviewed. Approve only when the
PR's `headRefOid` starts with that commit hash. STOP when the line names no
revision or names uncommitted changes, or when `headRefOid` doesn't match:
commits pushed after the review, and local commits not pushed yet, are code no
review has covered. Tell the user both commits; they either push the reviewed
commits or run `/review-pr` again on the new head.

State one line to the user: which PR (`#number — title`) at which commit you are
about to approve, then proceed.

## Step 2: Write the review body to a file

Write the review body to `/tmp/approve-pr-<PR>.md` using the `Write` tool (never
inline it into a shell command — reviews contain backticks, quotes, and `$` that
break shell quoting). The body is the full verbatim `review-pr` report, minus
what Step 0 leaves out.

## Step 2b: Check the issue drafts, file none

Approving ships the PR with its Deferred findings, and the posted review leaves
out its crash and security drafts. Don't file any of them: Step 4 tells the
user they need attention, and the user decides which to file. For each draft,
look for an issue that already covers it:

```bash
gh repo view questdb/questdb-enterprise --json nameWithOwner || echo "NO ACCESS"
gh issue list --repo questdb/questdb-enterprise --state all --search "<key words>" --limit 5
```

Without access, skip the search and say so in Step 4. When the user asks to file
a draft, file it in `questdb/questdb-enterprise` and nowhere else, as
review-pr's Deferred and Adjacent sections describe. Write its body to
`/tmp/approve-pr-<PR>-<ID>.md` with the `Write` tool: the draft, a Deferred one
opening with its Risk line.

```bash
gh label list --repo questdb/questdb-enterprise --search deferred --json name -q '.[].name' | grep -qx deferred \
  || gh label create deferred --repo questdb/questdb-enterprise \
       --description "Known bug a PR shipped with after a risk rating"
# A Deferred finding:
gh issue create --repo questdb/questdb-enterprise --title "<the draft's Problem line>" \
  --label bug --label deferred --body-file /tmp/approve-pr-<PR>-<ID>.md
# A crash draft:
gh issue create --repo questdb/questdb-enterprise --title "<the draft's Problem line>" \
  --label crash --body-file /tmp/approve-pr-<PR>-<ID>.md
# A security-sensitive draft:
gh issue create --repo questdb/questdb-enterprise --title "<the draft's Problem line>" \
  --label security --body-file /tmp/approve-pr-<PR>-<ID>.md
```

Label a missing test `deferred` alone, and add the label `security` to a
security-sensitive Deferred finding. Never mention these drafts or issues in the
review you post.

## Step 3: Post the approval

```bash
gh pr review <PR> --approve --body-file /tmp/approve-pr-<PR>.md
gh pr edit <PR> --add-label "QUEUED FOR MERGE"
```

Notes:
- GitHub does not allow approving your **own** PR. If `gh pr review --approve`
  fails for that reason, fall back to
  `gh pr comment <PR> --body-file /tmp/approve-pr-<PR>.md`, tell the user the PR
  could not be formally approved because it is self-authored, and still add the
  `QUEUED FOR MERGE` label.

## Step 4: Confirm

Report back in one or two lines:
- PR number + title, "approved" posted (or the fallback used).
- Whether the `QUEUED FOR MERGE` label was added.
- The PR URL.
- The issue drafts that need the user's attention: each Deferred finding,
  crash draft and security draft, one line each, with any existing issue that
  covers it. Ask which to file, and file only those the user names (Step 2b).
