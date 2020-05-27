import React from "react"
import styled from "styled-components"

import { color } from "utils"

const defaultProps = {
  type: "text",
}

type Props = {
  id?: string
  placeholder?: string
  title?: string
  type: "text" | "number"
}

const Wrapper = styled.input`
  height: 3rem;
  border: none;
  padding: 0 1rem;
  line-height: 1.5;
  outline: none;
  background: ${color("draculaSelection")};
  border-radius: 0.25rem;
  color: ${color("draculaForeground")};

  &:focus {
    box-shadow: inset 0 0 0 1px ${color("draculaForeground")};
  }

  &::placeholder {
    color: ${color("gray2")};
  }
`

export const Input = (props: Props) => <Wrapper {...props} />

Input.defaultProps = defaultProps
