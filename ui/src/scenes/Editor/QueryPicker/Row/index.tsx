import React, { useCallback } from "react"
import styled, { css } from "styled-components"
import { FileCode } from "@styled-icons/remix-line/FileCode"

import { Text, TransitionDuration } from "components"
import { QueryShape } from "types"
import { BusEvent, color } from "utils"

type Props = Readonly<{
  active: boolean
  hidePicker: () => void
  onHover: (query?: QueryShape) => void
  query: QueryShape
}>

const activeStyles = css`
  background: ${color("draculaSelection")};
`

const Wrapper = styled.div<{ active: boolean }>`
  display: flex;
  height: 2.4rem;
  padding: 0 0.6rem;
  line-height: 2.4rem;
  align-items: center;
  transition: background ${TransitionDuration.FAST}ms;
  cursor: pointer;
  user-select: none;

  ${({ active }) => active && activeStyles};

  > span:not(:last-child) {
    margin-right: 0.6rem;
  }
`

const Query = styled(Text)`
  flex: 1 1 auto;
  text-overflow: ellipsis;
  overflow: hidden;
  white-space: nowrap;
  opacity: 0.7;
`

const FileIcon = styled(FileCode)`
  height: 2.2rem;
  flex: 0 0 12px;
  margin: 0 0.6rem;
  color: ${color("draculaOrange")};
`

const Name = styled(Text)`
  flex: 0 0 auto;
`

const Row = ({ active, hidePicker, onHover, query }: Props) => {
  const handleClick = useCallback(() => {
    hidePicker()
    window.bus.trigger(BusEvent.MSG_EDITOR_SET, query.value)
  }, [hidePicker, query])
  const handleMouseEnter = useCallback(() => {
    onHover(query)
  }, [query, onHover])
  const handleMouseLeave = useCallback(() => {
    onHover()
  }, [onHover])

  return (
    <Wrapper
      active={active}
      onClick={handleClick}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <FileIcon size="12px" />

      {query.name && (
        <Name color="draculaForeground" size="sm">
          {query.name}
        </Name>
      )}

      <Query color="draculaForeground" size="sm">
        {query.value}
      </Query>
    </Wrapper>
  )
}

export default Row
