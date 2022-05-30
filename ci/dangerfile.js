import { danger, fail } from "danger"

const headRepoName = danger.github.pr.head.repo.full_name
const baseRepoName = danger.github.pr.base.repo.full_name
// Ignore forks for now until we find a good solution
if (headRepoName != baseRepoName) {
  // This is shown inline in the output and also integrates with the GitHub
  // Action reporting UI and produces a warning
  console.log("##[warning]Running from a forked repo. Danger won't be able to post comments on the main repo unless GitHub Actions are enabled on the fork, too.\033[0m")
  return
}

const supportedTypes = [
  "feat",
  "fix",
  "chore",
  "docs",
  "style",
  "refactor",
  "perf",
  "test",
  "build",
  "ci",
  "chore",
  "revert",
]

function validatePrTitle() {
  const prTitleRegex = new RegExp(`^(${supportedTypes.join("|")})(\(.+\))?\:.*`)

  const { draft, title } = danger.github.pr
  if (draft || title.match(prTitleRegex)) {
    return
  }

  warn(
    [
      "Please update the PR title.",
      "It should match this format: *type*: *description*",
      "Where *type* is one of\: " +
        supportedTypes.map((type) => `"${type}"`).join(", "),
    ].join("\n"),
  )
}

validatePrTitle()
