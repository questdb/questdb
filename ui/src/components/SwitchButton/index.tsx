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

import React, { useCallback } from "react"
import styled from "styled-components"
import { PrimaryButton, SecondaryButton } from "components"

type ItemValue = string

type Item = {
  text: string
  value: ItemValue
}

type Props = {
  items: Item[]
  value: ItemValue
  onSelect: (value: ItemValue) => void
}

const Wrapper = styled.div`
  display: flex;
  flex-direction: row;

  & > :first-child:not(:last-child) {
    border-radius: 5px 0 0 5px;
  }

  & > :last-child:not(:first-child) {
    border-radius: 0 5px 5px 0;
  }
`

const SwitchButton = ({ items, value, onSelect }: Props) => (
  <Wrapper>
    {items.map((item) => (
      <SwitchButtonElement
        item={item}
        key={item.value}
        onSelect={onSelect}
        value={value}
      />
    ))}
  </Wrapper>
)

type SwitchButtonElementProps = {
  item: Item
  value: ItemValue
  onSelect: (value: ItemValue) => void
}

const SwitchButtonElement = ({
  item,
  value,
  onSelect,
}: SwitchButtonElementProps) => {
  const Button = item.value === value ? PrimaryButton : SecondaryButton
  const handleSelect = useCallback(() => onSelect(item.value), [item, onSelect])

  return <Button onClick={handleSelect}>{item.text}</Button>
}

export { SwitchButton }
