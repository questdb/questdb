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
