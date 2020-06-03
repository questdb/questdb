import docsearch from "docsearch.js"
import React, { useCallback, useEffect, useState } from "react"
import { useSelector } from "react-redux"
import styled from "styled-components"
import { ControllerPlay } from "@styled-icons/entypo/ControllerPlay"
import { ControllerStop } from "@styled-icons/entypo/ControllerStop"
import { Plus } from "@styled-icons/entypo/Plus"

import {
  ErrorButton,
  Input,
  PaneTitle,
  PopperToggle,
  SecondaryButton,
  SuccessButton,
  useKeyPress,
} from "components"
import { selectors } from "store"
import { BusEvent } from "utils"

import QueryPicker from "./QueryPicker"

const Title = styled(PaneTitle)`
  .algolia-autocomplete {
    flex: 0 1 168px;
  }
`

const Separator = styled.div`
  flex: 1;
`

const DocsearchInput = styled(Input)`
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const QueryPickerButton = styled(SecondaryButton)`
  margin: 0 1rem;
  flex: 0 0 auto;
`

const Editor = () => {
  const [running, setRunning] = useState(false)
  const [popperActive, setPopperActive] = useState<boolean>()
  const handleClick = useCallback(() => {
    if (running) {
      window.bus.trigger(BusEvent.MSG_QUERY_CANCEL)
    } else {
      window.bus.trigger(BusEvent.MSG_EDITOR_EXECUTE)
    }
  }, [running])
  const handleToggle = useCallback((active) => {
    setPopperActive(active)
  }, [])
  const handleHidePicker = useCallback(() => {
    setPopperActive(false)
  }, [])
  const escPress = useKeyPress("Escape")
  const { savedQueries } = useSelector(selectors.console.getConfiguration)

  useEffect(() => {
    setPopperActive(false)
  }, [escPress])

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
    <Title>
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

      {savedQueries.length > 0 && (
        <PopperToggle
          active={popperActive}
          onToggle={handleToggle}
          trigger={
            <QueryPickerButton onClick={handleClick}>
              <Plus size="18px" />
              <span>Saved queries</span>
            </QueryPickerButton>
          }
        >
          <QueryPicker hidePicker={handleHidePicker} queries={savedQueries} />
        </PopperToggle>
      )}

      <Separator />

      <DocsearchInput
        id="docsearch-input"
        placeholder="Search documentation"
        title="Search..."
      />
    </Title>
  )
}

export default Editor
