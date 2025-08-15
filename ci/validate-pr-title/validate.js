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
  "wal",
  "parquet",
    "utils"
];

const errorMessage = `
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

/* The basic valid PR title formats are:
 * 1. allowedType(allowedSubtype): optional description
 * 2. allowedType: optional description
 *
 * consult ./validate.test.js for a full list
 * */
const prTitleRegex = new RegExp(
  `^(((?:${allowedTypes.join("|")})\\((?:${allowedSubTypes.join(
    "|"
  )})\\))|build): .*`
);

function validate({ title, onError }) {
  // Early return for title that matches predefined regex.
  // No action required in such case.
  if (title.match(prTitleRegex)) {
    return;
  }

  onError(errorMessage);
}

module.exports = {
  allowedTypes,
  allowedSubTypes,
  validate,
};
