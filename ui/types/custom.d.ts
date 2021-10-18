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

// eslint-disable-next-line no-var
declare var bus: {
  on: (event: string, callback: (event: null, payload: any) => void) => void
  trigger: (event: string, payload?: any) => void
}

interface ResizeObserverObserveOptions {
  box?: "content-box" | "border-box"
}

type ResizeObserverCallback = (
  entries: ResizeObserverEntry[],
  observer: ResizeObserver,
) => void

interface ResizeObserverEntry {
  readonly borderBoxSize: ResizeObserverEntryBoxSize
  readonly contentBoxSize: ResizeObserverEntryBoxSize
  readonly contentRect: DOMRectReadOnly
  readonly target: Element
}

interface ResizeObserverEntryBoxSize {
  blockSize: number
  inlineSize: number
}

declare class ResizeObserver {
  constructor(callback: ResizeObserverCallback)
  disconnect: () => void
  observe: (target: Element, options?: ResizeObserverObserveOptions) => void
  unobserve: (target: Element) => void
}

declare namespace Cypress {
  interface Chainable<Subject> {
    getByDataTest: (selector: string) => Cypress.Chainable<void>
    getByDataTestLike: (selector: string) => Cypress.Chainable<void>
    getByCustomData: (data: string, selector: string) => Cypress.Chainable<void>
    getSqlEditor: () => Cypress.Chainable<void>
    getSqlEditorValue: () => Cypress.Chainable<void>
    getGrid: () => Cypress.Chainable<void>
    getGridViewport: () => Cypress.Chainable<void>
    getGridHeader: () => Cypress.Chainable<void>
    getGridHeaderRow: () => Cypress.Chainable<void>
    getGridColumn: () => Cypress.Chainable<void>
    getGridRow: (active?: boolean) => Cypress.Chainable<void>
    execQuery: (query: string) => Cypress.Chainable<Response<any>>
  }
}
