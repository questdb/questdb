import React, { CSSProperties, forwardRef, Ref } from "react"

import { PaneWrapper } from "components"

import Ace from "./Ace"
import Menu from "./Menu"

type Props = Readonly<{
  style?: CSSProperties
}>

const Editor = ({
  innerRef,
  ...rest
}: Props & { innerRef: Ref<HTMLDivElement> }) => (
  <PaneWrapper ref={innerRef} {...rest}>
    <Menu />
    <Ace />
  </PaneWrapper>
)

const EditorWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <Editor {...props} innerRef={ref} />
)

export default forwardRef(EditorWithRef)
