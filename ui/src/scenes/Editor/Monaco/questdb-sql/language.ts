import { operators } from "./operators"
import * as monaco from "monaco-editor"
import { dataTypes, functions, keywords } from "@questdb/sql-grammar"

export const language: monaco.languages.IMonarchLanguage = {
  defaultToken: "",
  tokenPostfix: ".sql",
  ignoreCase: true,

  brackets: [
    { open: "[", close: "]", token: "delimiter.square" },
    { open: "(", close: ")", token: "delimiter.parenthesis" },
  ],
  dataTypes,
  keywords,
  operators,
  builtinFunctions: functions,
  builtinVariables: [
    // Configuration
    "@@DATEFIRST",
    "@@DBTS",
    "@@LANGID",
    "@@LANGUAGE",
    "@@LOCK_TIMEOUT",
    "@@MAX_CONNECTIONS",
    "@@MAX_PRECISION",
    "@@NESTLEVEL",
    "@@OPTIONS",
    "@@REMSERVER",
    "@@SERVERNAME",
    "@@SERVICENAME",
    "@@SPID",
    "@@TEXTSIZE",
    "@@VERSION",
    // Cursor
    "@@CURSOR_ROWS",
    "@@FETCH_STATUS",
    // Datetime
    "@@DATEFIRST",
    // Metadata
    "@@PROCID",
    // System
    "@@ERROR",
    "@@IDENTITY",
    "@@ROWCOUNT",
    "@@TRANCOUNT",
    // Stats
    "@@CONNECTIONS",
    "@@CPU_BUSY",
    "@@IDLE",
    "@@IO_BUSY",
    "@@PACKET_ERRORS",
    "@@PACK_RECEIVED",
    "@@PACK_SENT",
    "@@TIMETICKS",
    "@@TOTAL_ERRORS",
    "@@TOTAL_READ",
    "@@TOTAL_WRITE",
  ],
  pseudoColumns: ["$ACTION", "$IDENTITY", "$ROWGUID", "$PARTITION"],
  tokenizer: {
    root: [
      { include: "@comments" },
      { include: "@whitespace" },
      { include: "@pseudoColumns" },
      { include: "@numbers" },
      { include: "@strings" },
      { include: "@complexIdentifiers" },
      { include: "@scopes" },
      [/[;,.]/, "delimiter"],
      [/[()]/, "@brackets"],
      [
        /[\w@#$]+/,
        {
          cases: {
            "@operators": "operator",
            "@builtinVariables": "predefined",
            "@builtinFunctions": "predefined",
            "@keywords": "keyword",
            "@dataTypes": "dataType",
            "@default": "identifier",
          },
        },
      ],
      [/[<>=!%&+\-*/|~^]/, "operator"],
    ],
    whitespace: [[/\s+/, "white"]],
    comments: [
      [/--+.*/, "comment"],
      [/\/\*/, { token: "comment.quote", next: "@comment" }],
    ],
    comment: [
      [/[^*/]+/, "comment"],
      // Not supporting nested comments, as nested comments seem to not be standard?
      // i.e. http://stackoverflow.com/questions/728172/are-there-multiline-comment-delimiters-in-sql-that-are-vendor-agnostic
      // [/\/\*/, { token: 'comment.quote', next: '@push' }],    // nested comment not allowed :-(
      [/\*\//, { token: "comment.quote", next: "@pop" }],
      [/./, "comment"],
    ],
    pseudoColumns: [
      [
        /[$][A-Za-z_][\w@#$]*/,
        {
          cases: {
            "@pseudoColumns": "predefined",
            "@default": "identifier",
          },
        },
      ],
    ],
    numbers: [
      [/0[xX][0-9a-fA-F]*/, "number"],
      [/[$][+-]*\d*(\.\d*)?/, "number"],
      [/((\d+(\.\d*)?)|(\.\d+))([eE][\-+]?\d+)?/, "number"],
    ],
    strings: [
      [/N'/, { token: "string", next: "@string" }],
      [/'/, { token: "string", next: "@string" }],
    ],
    string: [
      [/[^']+/, "string"],
      [/''/, "string"],
      [/'/, { token: "string", next: "@pop" }],
    ],
    complexIdentifiers: [
      [/\[/, { token: "identifier.quote", next: "@bracketedIdentifier" }],
      [/"/, { token: "identifier.quote", next: "@quotedIdentifier" }],
    ],
    bracketedIdentifier: [
      [/[^\]]+/, "identifier"],
      [/]]/, "identifier"],
      [/]/, { token: "identifier.quote", next: "@pop" }],
    ],
    quotedIdentifier: [
      [/[^"]+/, "identifier"],
      [/""/, "identifier"],
      [/"/, { token: "identifier.quote", next: "@pop" }],
    ],
    scopes: [
      [/BEGIN\s+(DISTRIBUTED\s+)?TRAN(SACTION)?\b/i, "keyword"],
      [/BEGIN\s+TRY\b/i, { token: "keyword.try" }],
      [/END\s+TRY\b/i, { token: "keyword.try" }],
      [/BEGIN\s+CATCH\b/i, { token: "keyword.catch" }],
      [/END\s+CATCH\b/i, { token: "keyword.catch" }],
      [/(BEGIN|CASE)\b/i, { token: "keyword.block" }],
      [/END\b/i, { token: "keyword.block" }],
      [/WHEN\b/i, { token: "keyword.choice" }],
      [/THEN\b/i, { token: "keyword.choice" }],
    ],
  },
}
