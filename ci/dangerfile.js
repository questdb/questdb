import { danger, fail } from "danger";

const allowedTypes = [
  "feat",
  "fix",
  "chore",
  "docs",
  "style",
  "refactor",
  "perf",
  "test",
  "ci",
  "chore",
  "revert",
];

const allowedSubTypes = [
  "build",
  "sql",
  "log",
  "mig",
  "core",
  "ilp",
  "pgwire",
  "http",
  "conf",
  "ui",
];

const failMessage = `
Please update the PR title to match this format:
\`type(subType): description\`

Where \`type\` is one of:
${allowedTypes.map((t) => `\`${t}\``).join(", ")}

And: \`subType\` is one of:
${allowedSubTypes.map((t) => `\`${t}\``).join(", ")}

For Example:

\`\`\`
perf(sql): improve pattern matching performance for SELECT sub-queries
\`\`\`
`.trim();

function validatePrTitle() {
  const prTitleRegex = new RegExp(
    `^(((?:${allowedTypes.join("|")})\\((?:${allowedSubTypes.join("|")})\\))|build): .*`
  );

  const { title } = danger.github.pr;
  if (title.match(prTitleRegex)) {
    return;
  }

  fail(failMessage);
}

validatePrTitle();
