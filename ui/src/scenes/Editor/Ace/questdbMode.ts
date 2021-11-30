/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import ace, { Ace } from "ace-builds"
import "ace-builds/src-noconflict/mode-sql"
import "ace-builds/src-noconflict/theme-dracula"
import { constants, dataTypes, functions, keywords } from "@questdb/sql-grammar"

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
  const keywordMapper = this.createKeywordMapper(
    {
      "support.function": functions.join("|"),
      keyword: keywords.join("|"),
      "constant.language": constants.join("|"),
      "storage.type": dataTypes.join("|"),
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
        regex: "'.*?'",
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
        token: "entity.name.function", // geohash literal
        regex: "#{1,2}([a-zA-Z_$]|[a-zA-Z0-9_$])*\\b",
      },
      {
        token: keywordMapper,
        regex: "[a-zA-Z_$][a-zA-Z0-9_$]*\\b",
      },
      {
        token: "keyword.operator",
        regex: "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|=|!~",
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
