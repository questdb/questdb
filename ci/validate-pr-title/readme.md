This folder holds the validation rules applied to GitHub pull request titles, and
the job that reports on them.

- `validate.js` — the rules themselves: `type(subType): description`.
- `check.js` — reads the title from the workflow event, posts the `Danger` commit
  status, and leaves a comment explaining a rejection. The comment is updated in
  place while the title stays wrong and deleted once it is fixed.
- Tests run with node and need no dependencies: `node ./validate.test.js` and
  `node ./check.test.js`.

Run by [.github/workflows/pr_title.yml](../../.github/workflows/pr_title.yml) on
pull requests and on merge groups. It authenticates with the workflow's own
`GITHUB_TOKEN`, so it needs no bot account and no personal access token.

The status context is `Danger` for historical reasons and is required by the
`master` ruleset. Renaming it here without editing that ruleset in the same change
blocks every pull request, because the required check would never report.
