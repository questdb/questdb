import { ReactNode } from "react"
import styled from "styled-components"

type Props = Readonly<{
  children: ReactNode
  className?: string
}>

export const PaneWrapper = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
`
