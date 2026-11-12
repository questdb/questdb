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

The body you post is **the most recent `review-pr` report in this conversation**.

- If no review report exists in this conversation, STOP and tell the user to run
  `/skill:review-pr` first — do not fabricate a review.
- Do not shorten or rewrite the review; post it verbatim. You only prepend the
  author mention and leave out what stays out of public view: the Deferred
  section, each Adjacent draft the hostile-input rule covers, each
  security-sensitive draft (authentication or access-control bypass, data
  exposed across users, memory corruption), and any never-run line the coverage
  section lists for a failure the hostile-input rule covers. Tell the user what
  you left out.

## Step 1: Detect the PR and match the reviewed revision

The review's opening line names the PR it reviewed, if any, and the revision.
Use that PR. An explicit PR number or URL in the arguments overrides it, and
without either, take the PR of the checked-out branch. `$PR` holds its URL, which
every later `gh pr` command takes, and `$NUM` its number, which names the files.

```bash
PR='<PR from the arguments, else the PR the review names, else empty>'
PR=$(gh pr view ${PR:+"$PR"} --json url -q .url 2>/dev/null)
if [ -z "$PR" ]; then
  echo "No PR found for the current branch. Run 'gh pr checkout <n>' or pass a PR number."; exit 1
fi
NUM=$(gh pr view "$PR" --json number -q .number)
gh pr view "$PR" --json number,title,author,headRefName,headRefOid,url,state,labels
AUTHOR=$(gh pr view "$PR" --json author -q .author.login)
echo "Rejecting PR #$NUM by @$AUTHOR"
```

STOP unless `state` is `OPEN`.

When the PR's `headRefOid` doesn't start with the commit hash the review's
opening line names, or the line names uncommitted changes, the review covered
other code than the PR's head. STOP, tell the user both commits, and post only if
the user confirms.

State one line to the user: which PR (`#number — title`) and author you are about
to reject, then proceed.

## Step 2: Write the review body to a file

Write the review body to `/tmp/reject-pr-$NUM-<revision>.md`, where `<revision>`
is the reviewed commit's first 12 characters, using the `write` tool (never
inline it into a shell command — reviews contain backticks, quotes, and `$` that
break shell quoting). The first line must mention the author:

```
@<AUTHOR>

<the full verbatim review-pr report, minus what Step 0 leaves out>
```

## Step 3: Post the rejection

```bash
gh pr review "$PR" --request-changes --body-file /tmp/reject-pr-$NUM-<revision>.md

# Clear the READY label only if present (avoids a spurious error).
if gh pr view "$PR" --json labels -q '.labels[].name' | grep -qx "READY"; then
  gh pr edit "$PR" --remove-label "READY"
fi
```

Notes:
- GitHub does not allow requesting changes on your **own** PR. If `gh pr review
  --request-changes` fails for that reason, fall back to `gh pr comment "$PR"
  --body-file /tmp/reject-pr-$NUM-<revision>.md`, tell the user the PR could not
  be moved to "changes requested" because it is self-authored, and still clear
  the `READY` label.

## Step 4: Confirm

Report back briefly:
- PR number + title, author tagged, "changes requested" posted (or the fallback used).
- Whether the `READY` label was removed or was already absent.
- The PR URL.
- The issue drafts that need the user's attention: each Deferred finding,
  crash draft and security draft the post left out, one line each. Ask which
  to file, and file only those the user names, in `questdb/questdb-enterprise`
  with the labels review-pr's Deferred and Adjacent sections name.
