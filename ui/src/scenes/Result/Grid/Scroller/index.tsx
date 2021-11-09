import React, { Ref } from "react"
import { ScrollerProps } from "react-virtuoso"
import styled from "styled-components"

const ScrollerWrapper = styled.div``

const Scroller = React.forwardRef(
  ({ style, children }: ScrollerProps, ref: Ref<HTMLDivElement>) => {
    return (
      <ScrollerWrapper ref={ref} style={{ ...style }}>
        {children}
      </ScrollerWrapper>
    )
  },
)

Scroller.displayName = "Scroller"

export default Scroller
