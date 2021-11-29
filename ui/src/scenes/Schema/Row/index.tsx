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
import { SortDown } from "@styled-icons/boxicons-regular/SortDown"
import { CheckboxBlankCircle } from "@styled-icons/remix-line/CheckboxBlankCircle"
import { CodeSSlash } from "@styled-icons/remix-line/CodeSSlash"
import { Information } from "@styled-icons/remix-line/Information"
import { Table as TableIcon } from "@styled-icons/remix-line/Table"
import { PieChart } from "@styled-icons/remix-line/PieChart"

import {
  SecondaryButton,
  Text,
  TransitionDuration,
  IconWithTooltip,
} from "components"
import { color } from "utils"
import { useEditor } from "../../../providers"

type Props = Readonly<{
  className?: string
  designatedTimestamp?: string
  description?: string
  expanded?: boolean
  indexed?: boolean
  kind: "column" | "table"
  name: string
  onClick?: (event: MouseEvent) => void
  partitionBy?: string
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

const SortDownIcon = styled(SortDown)`
  color: ${color("draculaGreen")};
  margin-right: 0.8rem;
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

const PartitionByWrapper = styled.div`
  margin-right: 1rem;
  display: flex;
  align-items: center;
`

const PieChartIcon = styled(PieChart)`
  color: ${color("gray2")};
  margin-right: 0.5rem;
`

const Row = ({
  className,
  designatedTimestamp,
  description,
  expanded,
  kind,
  indexed,
  name,
  partitionBy,
  onClick,
  suffix,
  tooltip,
  type,
}: Props) => {
  const { insertTextAtCursor } = useEditor()

  const handlePlusButtonClick = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation()
      insertTextAtCursor(kind === "table" ? `'${name}'` : name)
    },
    [name, kind],
  )

  return (
    <Wrapper className={className} expanded={expanded} onClick={onClick}>
      <FlexRow>
        {kind === "table" && <TitleIcon size="18px" />}

        {kind === "column" && indexed && (
          <IconWithTooltip
            icon={<RocketIcon size="13px" />}
            placement="top"
            tooltip="Indexed"
          />
        )}

        {kind === "column" && !indexed && name === designatedTimestamp && (
          <IconWithTooltip
            icon={<SortDownIcon size="14px" />}
            placement="top"
            tooltip="Designated timestamp"
          />
        )}

        {kind === "column" && !indexed && type !== "TIMESTAMP" && (
          <DotIcon size="12px" />
        )}

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

        {kind === "table" && partitionBy !== "NONE" && (
          <PartitionByWrapper>
            <PieChartIcon size="14px" />
            <Text color="gray2">{partitionBy}</Text>
          </PartitionByWrapper>
        )}

        <PlusButton onClick={handlePlusButtonClick} size="sm" tooltip={tooltip}>
          <CodeSSlash size="16px" />
          <span>Add</span>
        </PlusButton>

        {tooltip && description && (
          <IconWithTooltip
            icon={
              <InfoIconWrapper>
                <InfoIcon size="10px" />
              </InfoIconWrapper>
            }
            placement="right"
            tooltip={description}
          />
        )}
      </FlexRow>

      {!tooltip && <Text color="draculaComment">{description}</Text>}
    </Wrapper>
  )
}

export default Row
