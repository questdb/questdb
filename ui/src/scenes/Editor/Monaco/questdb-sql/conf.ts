import * as monaco from "monaco-editor"

export const conf: monaco.languages.LanguageConfiguration = {
  /**
   * Override the default word definition regex to also allow single quotes and dots.
   * This way we can highlight table names escaped with quotes and the ones created from CSV files.
   * An additional example is a "bad integer" error, i.e. (20000) - needs brackets to be allowed as well.
   */
  wordPattern: /(-?\d*\.\d\w*)|([^\`\~\!\@\#\$\%\^\&\*\-\=\+\[\{\]\}\\\|\;\:\"\,\<\>\/\?\s]+)/g,
  comments: {
    lineComment: "--",
    blockComment: ["/*", "*/"],
  },
  brackets: [
    ["{", "}"],
    ["[", "]"],
    ["(", ")"],
  ],
  autoClosingPairs: [
    { open: "{", close: "}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
  surroundingPairs: [
    { open: "{", close: "}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],
}
