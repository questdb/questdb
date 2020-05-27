import docsearch from "docsearch.js"
import React, { useCallback, useEffect, useState } from "react"
import styled from "styled-components"
import { ControllerPlay, ControllerStop } from "@styled-icons/entypo"

import { ErrorButton, Input, PaneTitle, SuccessButton } from "components"
import { BusEvent } from "utils"

const Separator = styled.div`
  flex: 1;
`

const DocsearchInput = styled(Input)`
  width: 180px;
`

const Editor = () => {
  const [running, setRunning] = useState(false)
  const handleClick = useCallback(() => {
    if (running) {
      window.bus.trigger(BusEvent.MSG_QUERY_CANCEL)
    } else {
      window.bus.trigger(BusEvent.MSG_EDITOR_EXECUTE)
    }
  }, [running])

  useEffect(() => {
    docsearch({
      apiKey: "b2a69b4869a2a85284a82fb57519dcda",
      indexName: "questdb",
      inputSelector: "#docsearch-input",
      handleSelected: (input, event, suggestion, datasetNumber, context) => {
        if (context.selectionMethod === "click") {
          input.setVal("")
          const win = window.open(suggestion.url, "_blank")

          if (win) {
            win.focus()
          }
        }
      },
    })

    window.bus.on(BusEvent.MSG_QUERY_ERROR, () => {
      setRunning(false)
    })
    window.bus.on(BusEvent.MSG_QUERY_OK, () => {
      setRunning(false)
    })
    window.bus.on(BusEvent.MSG_QUERY_RUNNING, () => {
      setRunning(true)
    })
  }, [])

  return (
    <PaneTitle>
      {running && (
        <ErrorButton onClick={handleClick}>
          <ControllerStop size="18px" />
          <span>Cancel</span>
        </ErrorButton>
      )}

      {!running && (
        <SuccessButton onClick={handleClick}>
          <ControllerPlay size="18px" />
          <span>Run</span>
        </SuccessButton>
      )}

      <Separator />

      <DocsearchInput
        id="docsearch-input"
        placeholder="Search documentation..."
        title="Search..."
      />
    </PaneTitle>
  )
}

export default Editor
