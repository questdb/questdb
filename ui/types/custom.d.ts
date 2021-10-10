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
