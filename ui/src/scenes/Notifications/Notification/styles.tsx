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
