import styled from "styled-components"

import { Color } from "types"
import { color } from "utils"

export const Toast = styled.div<{ borderColor: Color }>`
  position: relative;
  display: flex;
  width: 340px;
  padding: 1rem;
  flex-direction: column;
  background: ${color("draculaBackgroundDarker")};
  border: 2px solid ${color("draculaSelection")};

  &:before {
    position: absolute;
    display: block;
    content: " ";
    width: 3px;
    top: 0;
    right: 0;
    bottom: -1px;
    background: ${({ borderColor }) => color(borderColor)};
  }
`
