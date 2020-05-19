import React, { MouseEvent, ReactNode, useCallback } from "react"
import styled from "styled-components"
import { Code, Info } from "@styled-icons/entypo"

import { PopperHover, SecondaryButton, Text, Tooltip } from "components"
import { color } from "utils"

type Props = Readonly<{
  children?: ReactNode
  className?: string
  description?: string
  expanded?: boolean
  name: string
  onClick?: (event: MouseEvent) => void
  tooltip?: boolean
  type?: string
}>

const Type = styled(Text)`
  flex: 0;
  transition: all 0.2s;
`

const PlusButton = styled(SecondaryButton)`
  position: absolute;
  right: ${({ tooltip }: Pick<Props, "tooltip">) =>
    tooltip ? "2.5rem" : "1rem"};
  margin-left: 1rem;
  opacity: 0;
  transition: all 0.2s;
`

const Wrapper = styled.div<Pick<Props, "expanded">>`
  position: relative;
  display: flex;
  flex-direction: column;
  padding: 0.5rem 1rem;
  transition: all 0.2s;

  &:hover ${/* sc-selector */ PlusButton} {
    opacity: 1;
  }

  &:hover {
    background: ${color("draculaSelection")};
  }

  &:hover ${/* sc-selector */ Type} {
    opacity: 0;
  }
`

const FlexRow = styled.div`
  display: flex;
`

const Name = styled(Text)`
  flex: 0;
`

const Spacer = styled.span`
  flex: 1;
`

const InfoIcon = styled(Info)`
  height: 100%;
  width: 100%;
  padding-left: 1rem;
  color: ${color("draculaPurple")};

  &:hover {
    cursor: default;
  }
`

const Row = ({
  children,
  className,
  description,
  expanded,
  name,
  onClick,
  tooltip,
  type,
}: Props) => {
  const handleClick = useCallback((event: MouseEvent) => {
    event.stopPropagation()
    window.bus.trigger("editor.insert.column", name)
  }, [])

  return (
    <Wrapper className={className} expanded={expanded} onClick={onClick}>
      <FlexRow>
        {children}
        <Name color="draculaForeground">{name}</Name>

        <Spacer />

        {type && (
          <Type _style="italic" color="draculaPink">
            {type}
          </Type>
        )}

        <PlusButton onClick={handleClick} size="sm" tooltip={tooltip}>
          <Code size="16px" />
          Add
        </PlusButton>

        {tooltip && description && (
          <PopperHover
            modifiers={[
              {
                name: "offset",
                options: {
                  offset: [15, 0],
                },
              },
            ]}
            placement="top-start"
            trigger={<InfoIcon onClick={handleClick} size="10px" />}
          >
            <Tooltip>{description}</Tooltip>
          </PopperHover>
        )}
      </FlexRow>

      {!tooltip && <Text color="draculaComment">{description}</Text>}
    </Wrapper>
  )
}

export default Row
