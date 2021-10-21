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

import styled from "styled-components"
import { bezierTransition } from "components"
import { color } from "utils"

export const Wrapper = styled.div`
  display: flex;
  align-items: center;
  border-right: none;
  width: 100%;
  height: 4rem;
  border-bottom: 1px ${color("draculaBackgroundDarker")} solid;
  padding: 0 1rem;

  ${bezierTransition};
`

export const Content = styled.div`
  display: flex;
  margin-left: 0.5rem;
  white-space: nowrap;
`

export const SideContent = styled.div`
  margin-left: auto;
  overflow: hidden;
  text-overflow: ellipsis;
`
