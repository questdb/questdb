import React, { forwardRef, Ref, useEffect, useState } from "react"
import styled from "styled-components"
import { DownArrowSquare } from "@styled-icons/boxicons-solid/DownArrowSquare"
import { UpArrowSquare } from "@styled-icons/boxicons-solid/UpArrowSquare"

import { Text, useKeyPress } from "components"
import { QueryShape } from "types"
import { BusEvent, color } from "utils"

import Row from "./Row"

type Props = {
  hidePicker: () => void
  queries: QueryShape[]
  ref: Ref<HTMLDivElement>
}

const Wrapper = styled.div`
  display: flex;
  max-height: 600px;
  width: 600px;
  padding: 0.6rem 0;
  flex-direction: column;
  background: ${color("draculaBackgroundDarker")};
  box-shadow: ${color("black")} 0px 5px 8px;
  border: 1px solid ${color("black")};
  border-radius: 4px;
`

const Helper = styled(Text)`
  padding: 1rem;
  text-align: center;
  opacity: 0.5;
`

const Esc = styled(Text)`
  padding: 0 2px;
  background: ${color("draculaForeground")};
  border-radius: 2px;
`

const QueryPicker = ({ hidePicker, queries, ref }: Props) => {
  const downPress = useKeyPress("ArrowDown")
  const upPress = useKeyPress("ArrowUp")
  const enterPress = useKeyPress("Enter", { preventDefault: true })
  const [cursor, setCursor] = useState(0)
  const [hovered, setHovered] = useState<QueryShape | undefined>()

  useEffect(() => {
    if (queries.length && downPress) {
      setCursor((prevState) =>
        prevState < queries.length - 1 ? prevState + 1 : prevState,
      )
    }
  }, [downPress, queries])

  useEffect(() => {
    if (queries.length && upPress) {
      setCursor((prevState) => (prevState > 0 ? prevState - 1 : prevState))
    }
  }, [upPress, queries])

  useEffect(() => {
    if (enterPress && queries[cursor]) {
      hidePicker()
      window.bus.trigger(BusEvent.MSG_EDITOR_SET, queries[cursor].value)
    }
  }, [cursor, enterPress, hidePicker, queries])

  useEffect(() => {
    if (hovered) {
      setCursor(queries.indexOf(hovered))
    }
  }, [hovered, queries])

  return (
    <Wrapper ref={ref}>
      <Helper _style="italic" color="draculaForeground" size="xs">
        Navigate the list with <UpArrowSquare size="16px" />
        <DownArrowSquare size="16px" /> keys, exit with&nbsp;
        <Esc _style="normal" size="ms" weight={700}>
          Esc
        </Esc>
      </Helper>

      {queries.map((query, i) => (
        <Row
          active={i === cursor}
          hidePicker={hidePicker}
          key={query.name}
          onHover={setHovered}
          query={query}
        />
      ))}
    </Wrapper>
  )
}

const QueryPickerWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <QueryPicker {...props} ref={ref} />
)

export default forwardRef(QueryPickerWithRef)
