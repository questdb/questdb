import docsearch from "docsearch.js"

const search = docsearch({
  apiKey: "b2a69b4869a2a85284a82fb57519dcda",
  indexName: "questdb",
  inputSelector: "#help-input",
  algoliaOptions: {},
})

search.autocomplete.off("autocomplete:selected")
search.autocomplete.on("autocomplete:selected", function (event, data) {
  window.open(data.url, "_blank")
})
