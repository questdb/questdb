import React, { MouseEvent, ReactNode, useCallback } from "react"
import styled from "styled-components"
import { Code } from "@styled-icons/entypo/Code"
import { Info } from "@styled-icons/entypo/Info"

import {
  PopperHover,
  SecondaryButton,
  Text,
  Tooltip,
  TransitionDuration,
} from "components"
import { color } from "utils"

type Props = Readonly<{
  className?: string
  description?: string
  expanded?: boolean
  name: string
  onClick?: (event: MouseEvent) => void
  prefix?: ReactNode
  suffix?: ReactNode
  tooltip?: boolean
  type?: string
}>

const Type = styled(Text)`
  margin-right: 1rem;
  flex: 0;
  transition: opacity ${TransitionDuration.REG}ms;
`

const PlusButton = styled(SecondaryButton)<Pick<Props, "tooltip">>`
  position: absolute;
  right: ${({ tooltip }) => (tooltip ? "3rem" : "1rem")};
  margin-left: 1rem;
  opacity: 0;
`

const Wrapper = styled.div<Pick<Props, "expanded">>`
  position: relative;
  display: flex;
  flex-direction: column;
  padding: 0.5rem 0;
  padding-left: 1rem;
  transition: background ${TransitionDuration.REG}ms;

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
  align-items: center;
`

const Name = styled(Text)`
  flex: 0;
`

const Spacer = styled.span`
  flex: 1;
`

const InfoIcon = styled(Info)`
  color: ${color("draculaPurple")};
`

const InfoIconWrapper = styled.div`
  display: flex;
  padding: 0 1rem;
  align-items: center;
  justify-content: center;
`

const Row = ({
  className,
  description,
  expanded,
  name,
  onClick,
  prefix,
  suffix,
  tooltip,
  type,
}: Props) => {
  const handleClick = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation()
      window.bus.trigger("editor.insert.column", name)
    },
    [name],
  )

  return (
    <Wrapper className={className} expanded={expanded} onClick={onClick}>
      <FlexRow>
        {prefix}
        <Name color="draculaForeground">{name}</Name>
        {suffix}

        <Spacer />

        {type && (
          <Type _style="italic" color="draculaPink" transform="lowercase">
            {type}
          </Type>
        )}

        <PlusButton onClick={handleClick} size="sm" tooltip={tooltip}>
          <Code size="16px" />
          <span>Add</span>
        </PlusButton>

        {tooltip && description && (
          <PopperHover
            modifiers={[
              {
                name: "offset",
                options: {
                  offset: [-15, 0],
                },
              },
            ]}
            placement="right"
            trigger={
              <InfoIconWrapper>
                <InfoIcon size="10px" />
              </InfoIconWrapper>
            }
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
