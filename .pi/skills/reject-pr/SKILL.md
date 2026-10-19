---
name: reject-pr
description: Reject the pull request for the currently checked-out branch. Posts the review from this conversation as a change-request review that tags the PR author, sets the PR to "changes requested", and clears the READY label. Use after review-pr when you decide to reject.
allowed-tools: bash read write
metadata:
  argument-hint: "[optional PR number or URL to override auto-detection]"
---

# Reject a QuestDB pull request

Reject the pull request for the branch that is currently checked out. This skill
is meant to be run **right after `review-pr`** in the same conversation: it takes
the review you just produced and turns it into a blocking change-request on the
PR. Do not re-review here — post the existing review.

Actions performed, in order:
1. Post the review as the PR's review body (this is the "PR comment"), tagging
   the PR author.
2. Request changes (sets the PR to the "changes requested" state).
3. Clear the `READY` label.

## Step 0: Locate the review to post

The body you post is **the most recent `review-pr` report in this conversation**
(its Critical / Moderate / Minor / Coverage map / Summary output).

- If no review report exists in this conversation, STOP and tell the user to run
  `/skill:review-pr` first — do not fabricate a review.
- Do not shorten or rewrite the review; post it verbatim. You only prepend the
  author mention.

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
AUTHOR=$(gh pr view "$PR" --json author -q .author.login)
echo "Rejecting PR #$PR by @$AUTHOR"
```

State one line to the user: which PR (`#number — title`) and author you are about
to reject, then proceed.

## Step 2: Write the review body to a file

Write the review body to `/tmp/reject-pr-$PR.md` using the `write` tool (never
inline it into a shell command — reviews contain backticks, quotes, and `$` that
break shell quoting). The first line must mention the author:

```
@<AUTHOR>

<the full verbatim review-pr report>
```

## Step 3: Post the rejection

```bash
gh pr review "$PR" --request-changes --body-file /tmp/reject-pr-$PR.md

# Clear the READY label only if present (avoids a spurious error).
if gh pr view "$PR" --json labels -q '.labels[].name' | grep -qx "READY"; then
  gh pr edit "$PR" --remove-label "READY"
fi
```

Notes:
- GitHub does not allow requesting changes on your **own** PR. If
  `gh pr review --request-changes` fails for that reason, fall back to
  `gh pr comment "$PR" --body-file /tmp/reject-pr-$PR.md`, tell the user the PR
  could not be moved to "changes requested" because it is self-authored, and
  still clear the `READY` label.

## Step 4: Confirm

Report back in one or two lines:
- PR number + title, author tagged, "changes requested" posted (or the fallback used).
- Whether the `READY` label was removed or was already absent.
- The PR URL.
