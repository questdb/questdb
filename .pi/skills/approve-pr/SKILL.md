---
name: approve-pr
description: Approve the pull request for the currently checked-out branch. Posts the review from this conversation as an approving review and adds the "QUEUED FOR MERGE" label. Use after review-pr when you decide to approve.
allowed-tools: bash read write
metadata:
  argument-hint: "[optional PR number or URL to override auto-detection]"
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

## Step 0: Confirm the review actually clears the bar

Only approve if the most recent `review-pr` report in this conversation has a
verdict of **approve** with ZERO open Critical findings and ZERO open
correctness / performance / IO / resource findings (per the review-pr
correctness & performance gate). If any such finding is still open, do NOT
approve — tell the user the PR does not meet the bar and suggest `reject-pr`.

- If no review report exists in this conversation, STOP and tell the user to run
  `/skill:review-pr` first — do not approve unreviewed code.
- Post the review verbatim; do not shorten or rewrite it.

## Step 1: Detect the PR

Auto-detect the PR from the checked-out branch. An explicit PR number/URL in the
arguments overrides detection.

```bash
PR='<explicit PR number/URL from arguments, else empty>'
[ -z "$PR" ] && PR=$(gh pr view --json number -q .number 2>/dev/null)
if [ -z "$PR" ]; then
  echo "No PR found for the current branch. Run 'gh pr checkout <n>' or pass a PR number."; exit 1
fi
gh pr view "$PR" --json number,title,author,headRefName,url,state,labels
echo "Approving PR #$PR"
```

State one line to the user: which PR (`#number — title`) you are about to approve,
then proceed.

## Step 2: Write the review body to a file

Write the review body to `/tmp/approve-pr-$PR.md` using the `write` tool (never
inline it into a shell command — reviews contain backticks, quotes, and `$` that
break shell quoting). The body is the full verbatim `review-pr` report.

## Step 3: Post the approval

```bash
gh pr review "$PR" --approve --body-file /tmp/approve-pr-$PR.md
gh pr edit "$PR" --add-label "QUEUED FOR MERGE"
```

Notes:
- GitHub does not allow approving your **own** PR. If `gh pr review --approve`
  fails for that reason, fall back to
  `gh pr comment "$PR" --body-file /tmp/approve-pr-$PR.md`, tell the user the PR
  could not be formally approved because it is self-authored, and still add the
  `QUEUED FOR MERGE` label.

## Step 4: Confirm

Report back in one or two lines:
- PR number + title, "approved" posted (or the fallback used).
- Whether the `QUEUED FOR MERGE` label was added.
- The PR URL.
