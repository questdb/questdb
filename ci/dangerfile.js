// Validate PR titles
function validatePrTitle() {
  const prTitleRegex = /(feat|fix|chore|docs|style|refactor|perf|test|build|ci|chore|revert)(\(.+\))?\:.*/g
  const prTitle = danger.github.pr.title
  const isDraft = danger.github.pr.draft

  if (isDraft || prTitle.match(prTitleRegex)) {
    return
  }

  message('Please update the PR title. Titles need to match this format: <type>: <description>')
}

validatePrTitle()
