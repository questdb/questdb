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
  "wal",
];

module.exports = {
  allowedTypes,
  allowedSubTypes,
};
