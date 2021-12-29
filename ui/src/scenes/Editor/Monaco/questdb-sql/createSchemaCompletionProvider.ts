import { Table } from "../../../../utils"
import * as monaco from "monaco-editor"
import { CompletionItemKind } from "./types"

export const createSchemaCompletionProvider = (questDBTables: Table[] = []) => {
  const completionProvider: monaco.languages.CompletionItemProvider = {
    triggerCharacters: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz '".split(
      "",
    ),
    provideCompletionItems(model, position) {
      const textUntilPosition = model.getValueInRange({
        startLineNumber: 1,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      })

      const word = model.getWordUntilPosition(position)

      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      }

      if (
        word.word ||
        /(FROM|INTO|TABLE) $/gim.test(textUntilPosition) ||
        (/'$/gim.test(textUntilPosition) && !textUntilPosition.endsWith("= '"))
      ) {
        return {
          suggestions: questDBTables.map((item) => {
            return {
              label: item.name,
              kind: CompletionItemKind.Class,
              insertText:
                textUntilPosition.substr(-1) === "'"
                  ? item.name
                  : `'${item.name}'`,
              range,
            }
          }),
        }
      }
    },
  }

  return completionProvider
}
