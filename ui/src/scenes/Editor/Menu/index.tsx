import docsearch from "docsearch.js"
import React, { useCallback, useEffect, useState } from "react"
import { useDispatch, useSelector } from "react-redux"
import { CSSTransition } from "react-transition-group"
import styled from "styled-components"
import { Add } from "@styled-icons/remix-line/Add"
import { Close as _CloseIcon } from "@styled-icons/remix-line/Close"
import { Menu as _MenuIcon } from "@styled-icons/remix-fill/Menu"
import { Play } from "@styled-icons/remix-line/Play"
import { Stop } from "@styled-icons/remix-line/Stop"

import {
  ErrorButton,
  Input,
  PaneMenu,
  PopperToggle,
  SecondaryButton,
  SuccessButton,
  TransitionDuration,
  TransparentButton,
  useKeyPress,
  useScreenSize,
} from "components"
import { actions, selectors } from "store"
import { color } from "utils"

import QueryPicker from "../QueryPicker"

const Wrapper = styled(PaneMenu)`
  z-index: 15;

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

const MenuIcon = styled(_MenuIcon)`
  color: ${color("draculaForeground")};
`

const CloseIcon = styled(_CloseIcon)`
  color: ${color("draculaForeground")};
`

const SideMenuMenuButton = styled(TransparentButton)`
  padding: 0;
  margin-left: 1rem;

  .fade-enter {
    opacity: 0;
  }

  .fade-enter-active {
    opacity: 1;
    transition: opacity ${TransitionDuration.REG}ms;
  }

  .fade-exit {
    opacity: 0;
  }

  .fade-exit-active {
    opacity: 1;
    transition: opacity ${TransitionDuration.REG}ms;
  }
`

const Menu = () => {
  const dispatch = useDispatch()
  const [popperActive, setPopperActive] = useState<boolean>()
  const escPress = useKeyPress("Escape")
  const { savedQueries } = useSelector(selectors.console.getConfiguration)
  const running = useSelector(selectors.query.getRunning)
  const opened = useSelector(selectors.console.getSideMenuOpened)
  const { sm } = useScreenSize()

  const handleClick = useCallback(() => {
    dispatch(actions.query.toggleRunning())
  }, [dispatch])
  const handleToggle = useCallback((active) => {
    setPopperActive(active)
  }, [])
  const handleHidePicker = useCallback(() => {
    setPopperActive(false)
  }, [])
  const handleSideMenuButtonClick = useCallback(() => {
    dispatch(actions.console.toggleSideMenu())
  }, [dispatch])

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
  }, [])

  useEffect(() => {
    if (!sm && opened) {
      dispatch(actions.console.toggleSideMenu())
    }
  }, [dispatch, opened, sm])

  return (
    <Wrapper>
      {running && (
        <ErrorButton onClick={handleClick}>
          <Stop size="18px" />
          <span>Cancel</span>
        </ErrorButton>
      )}

      {!running && (
        <SuccessButton onClick={handleClick}>
          <Play size="18px" />
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
              <Add size="18px" />
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

      {sm && (
        <SideMenuMenuButton onClick={handleSideMenuButtonClick}>
          <CSSTransition
            classNames="fade"
            in={opened}
            timeout={TransitionDuration.REG}
          >
            {opened ? <CloseIcon size="26px" /> : <MenuIcon size="26px" />}
          </CSSTransition>
        </SideMenuMenuButton>
      )}
    </Wrapper>
  )
}

export default Menu
