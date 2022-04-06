import { IRange } from "monaco-editor"
import * as monaco from "monaco-editor"
import { formatSql } from "../../../../utils"

export const documentFormattingEditProvider = {
  provideDocumentFormattingEdits(
    model: monaco.editor.IModel,
    options: monaco.languages.FormattingOptions,
  ) {
    const formatted = formatSql(model.getValue(), {
      indent: " ".repeat(options.tabSize),
    })
    return [
      {
        range: model.getFullModelRange(),
        text: formatted,
      },
    ]
  },
}

export const documentRangeFormattingEditProvider = {
  provideDocumentRangeFormattingEdits(
    model: monaco.editor.IModel,
    range: IRange,
    options: monaco.languages.FormattingOptions,
  ) {
    const formatted = formatSql(model.getValueInRange(range), {
      indent: " ".repeat(options.tabSize),
    })
    return [
      {
        range,
        text: formatted,
      },
    ]
  },
}
