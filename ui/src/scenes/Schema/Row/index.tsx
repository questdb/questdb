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

import React, { MouseEvent, ReactNode, useCallback } from "react"
import styled from "styled-components"
import { Rocket } from "@styled-icons/boxicons-regular/Rocket"
import { CheckboxBlankCircle } from "@styled-icons/remix-line/CheckboxBlankCircle"
import { CodeSSlash } from "@styled-icons/remix-line/CodeSSlash"
import { Information } from "@styled-icons/remix-line/Information"
import { Table as TableIcon } from "@styled-icons/remix-line/Table"

import {
  PopperHover,
  SecondaryButton,
  Text,
  Tooltip,
  TransitionDuration,
} from "components"
import { color } from "utils"
import { BusEvent } from "../../../consts"

type Props = Readonly<{
  className?: string
  description?: string
  expanded?: boolean
  indexed?: boolean
  kind: "column" | "table"
  name: string
  onClick?: (event: MouseEvent) => void
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

  &:hover
    ${/* sc-selector */ PlusButton},
    &:active
    ${/* sc-selector */ PlusButton} {
    opacity: 1;
  }

  &:hover,
  &:active {
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

const Spacer = styled.span`
  flex: 1;
`

const InfoIcon = styled(Information)`
  color: ${color("draculaPurple")};
`

const RocketIcon = styled(Rocket)`
  color: ${color("draculaOrange")};
  margin-right: 1rem;
`

const DotIcon = styled(CheckboxBlankCircle)`
  color: ${color("gray2")};
  margin-right: 1rem;
`

const TitleIcon = styled(TableIcon)`
  min-height: 18px;
  min-width: 18px;
  margin-right: 1rem;
  color: ${color("draculaCyan")};
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
  kind,
  indexed,
  name,
  onClick,
  suffix,
  tooltip,
  type,
}: Props) => {
  const handlePlusButtonClick = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation()
      window.bus.trigger(
        BusEvent.MSG_EDITOR_INSERT_COLUMN,
        kind === "table" ? `'${name}'` : name,
      )
    },
    [name, kind],
  )

  return (
    <Wrapper className={className} expanded={expanded} onClick={onClick}>
      <FlexRow>
        {kind === "table" && <TitleIcon size="18px" />}

        {kind === "column" && indexed && (
          <PopperHover
            modifiers={[
              {
                name: "offset",
                options: {
                  offset: [-15, 0],
                },
              },
            ]}
            placement="top"
            trigger={<RocketIcon size="13px" />}
          >
            <Tooltip>Indexed</Tooltip>
          </PopperHover>
        )}

        {kind === "column" && !indexed && <DotIcon size="12px" />}

        <Text color="draculaForeground" ellipsis>
          {name}
        </Text>
        {suffix}

        <Spacer />

        {type && (
          <Type _style="italic" color="draculaPink" transform="lowercase">
            {type}
          </Type>
        )}

        <PlusButton onClick={handlePlusButtonClick} size="sm" tooltip={tooltip}>
          <CodeSSlash size="16px" />
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
