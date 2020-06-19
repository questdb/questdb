import ace, { Ace } from "ace-builds"
import "ace-builds/src-noconflict/mode-sql"
import "ace-builds/src-noconflict/theme-dracula"

type Mapper = (name: string) => void

type Rule = Readonly<{
  end?: string
  regex?: string
  start?: string
  token: string | Mapper
}>

interface HighlightRules {
  createKeywordMapper: (
    map: Record<string, any>,
    defaultToken: string,
    ignoreCase: boolean,
    splitChar?: boolean,
  ) => Mapper
  normalizeRules: () => void
  $rules: Record<string, Rule[]>
}

const { TextHighlightRules } = ace.require("ace/mode/text_highlight_rules") as {
  TextHighlightRules: HighlightRules
}
const { Mode: SqlMode } = ace.require("ace/mode/sql") as { Mode: unknown }
const oop = ace.require("ace/lib/oop") as { inherits: (a: any, b: any) => void }

const QuestDBHighlightRules = function (this: HighlightRules) {
  const keywords = [
    "add",
    "alter",
    "and",
    "as",
    "asc",
    "asof",
    "by",
    "cache",
    "capacity",
    "case",
    "column",
    "columns",
    "copy",
    "create",
    "cross",
    "database",
    "default",
    "delete",
    "desc",
    "distinct",
    "drop",
    "else",
    "end",
    "foreign",
    "from",
    "grant",
    "group",
    "if",
    "index",
    "inner",
    "insert",
    "into",
    "join",
    "key",
    "latest",
    "left",
    "limit",
    "nan",
    "natural",
    "nocache",
    "not",
    "null",
    "on",
    "or",
    "order",
    "outer",
    "over",
    "partition",
    "primary",
    "references",
    "rename",
    "right",
    "sample",
    "select",
    "show",
    "splice",
    "table",
    "tables",
    "then",
    "truncate",
    "type",
    "union",
    "update",
    "values",
    "when",
    "where",
    "with",
  ]
  const builtinConstants = ["false", "true"]
  const builtinFunctions = [
    "abs",
    "all_tables",
    "avg",
    "coalesce",
    "concat",
    "count",
    "dateadd",
    "datediff",
    "day",
    "day_of_week",
    "day_of_week_sunday_first",
    "days_in_month",
    "first",
    "format",
    "hour",
    "ifnull",
    "is_leap_year",
    "isnull",
    "ksum",
    "last",
    "lcase",
    "len",
    "length",
    "long_sequence",
    "max",
    "micros",
    "mid",
    "millis",
    "min",
    "minute",
    "month",
    "now",
    "nsum",
    "nvl",
    "rank",
    "rnd_bin",
    "rnd_boolean",
    "rnd_byte",
    "rnd_char",
    "rnd_date",
    "rnd_double",
    "rnd_float",
    "rnd_int",
    "rnd_long",
    "rnd_long256",
    "rnd_short",
    "rnd_str",
    "rnd_symbol",
    "rnd_timestamp",
    "round",
    "round_down",
    "round_half_even",
    "round_up",
    "second",
    "sum",
    "sysdate",
    "systimestamp",
    "tables_columns",
    "timestamp_sequence",
    "to_date",
    "to_str",
    "to_timestamp",
    "ucase",
    "year",
  ]
  const dataTypes = [
    "binary",
    "date",
    "double",
    "float",
    "int",
    "long",
    "long256",
    "short",
    "string",
    "symbol",
    "timestamp",
  ]
  const keywordMapper = this.createKeywordMapper(
    {
      keyword: [keywords, builtinFunctions, builtinConstants, dataTypes]
        .map((k) => k.join("|"))
        .join("|"),
    },
    "identifier",
    true,
  )

  this.$rules = {
    start: [
      {
        token: "comment",
        regex: "--.*$",
      },
      {
        token: "comment",
        start: "/\\*",
        end: "\\*/",
      },
      {
        token: "string", // " string
        regex: '".*?"',
      },
      {
        token: "constant", // ' string
        regex: "'.*?'",
      },
      {
        token: "string",
        regex: "`.*?`",
      },
      {
        token: "entity.name.function", // float
        regex: "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b",
      },
      {
        token: keywordMapper,
        regex: "[a-zA-Z_$][a-zA-Z0-9_$]*\\b",
      },
      {
        token: "keyword.operator",
        regex: "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|=",
      },
      {
        token: "paren.lparen",
        regex: "[\\(]",
      },
      {
        token: "paren.rparen",
        regex: "[\\)]",
      },
      {
        token: "text",
        regex: "\\s+",
      },
    ],
  }
  this.normalizeRules()
}

interface QuestDBMode {
  HighlightRules: typeof QuestDBHighlightRules
}

const QuestDBMode = (function (this: QuestDBMode) {
  this.HighlightRules = QuestDBHighlightRules
} as any) as { new (): QuestDBMode }

oop.inherits(QuestDBHighlightRules, TextHighlightRules)
oop.inherits(QuestDBMode, SqlMode)

const _mode = (new QuestDBMode() as unknown) as Ace.SyntaxMode

export default _mode
