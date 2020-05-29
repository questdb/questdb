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

declare var BACKEND_PORT: number // eslint-disable-line no-var

// eslint-disable-next-line no-var
declare var bus: {
  on: (event: string, callback: () => void) => void
  trigger: (event: string, payload?: string) => void
}
