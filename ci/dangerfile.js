import { danger, fail } from "danger"

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
      "Where *type* is one of: " +
        supportedTypes.map((type) => `"${type}"`).join(", "),
    ].join("\n"),
  )
}

validatePrTitle()
