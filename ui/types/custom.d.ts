type Suggestion = Readonly<{ url: string }>

type Context = Readonly<{
  selectionMethod: "click" | "enterKey" | "tabKey" | "blur"
}>

type Config = {
  apiKey: string
  indexName: string
  inputSelector: string
  handleSelected: (
    input: HTMLInputElement & { setVal: (value: string) => void },
    event: Event,
    suggestion: Suggestion,
    datasetNumber: number,
    context: Context,
  ) => void
}

declare module "docsearch.js" {
  export default function docsearch(config: Config): void
}
