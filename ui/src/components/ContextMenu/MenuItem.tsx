/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import React, { PropsWithChildren } from "react"
import styled from "styled-components"
import { MenuItem as ReactMenuItem, MenuItemProps } from "react-contextmenu"
import { color } from "utils"

const StyledMenuItem = styled(ReactMenuItem)`
  color: ${color("draculaForeground")};
  cursor: pointer;
  padding: 0.5rem 1rem;

  &:hover {
    background: ${color("draculaSelection")};
  }
`

const StyledDivider = styled(ReactMenuItem)`
  background: ${color("draculaForeground")};
  height: 1px;
  width: 100%;
  margin: 0.5rem 0;
  padding: 0 1rem;
`

type Props = {} & MenuItemProps

const MenuItem = ({ children, divider, ...rest }: PropsWithChildren<Props>) => {
  const Component = divider ? StyledDivider : StyledMenuItem

  return <Component {...rest}>{children}</Component>
}

export default MenuItem
