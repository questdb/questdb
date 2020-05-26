import docsearch from "docsearch.js"

docsearch({
  apiKey: "b2a69b4869a2a85284a82fb57519dcda",
  indexName: "questdb",
  inputSelector: "#help-input",
  algoliaOptions: {},
  handleSelected: function (input, event, suggestion, datasetNumber, context) {
    if (context.selectionMethod === "click") {
      input.setVal("")

      const windowReference = window.open(suggestion.url, "_blank")
      windowReference.focus()
    }
  },
})
