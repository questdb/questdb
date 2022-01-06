import React from "react"

export const WrapWithIf: React.FunctionComponent<{
  condition: boolean
  wrapper: (children: React.ReactElement) => JSX.Element
  children: React.ReactElement
}> = ({ condition, wrapper, children }) =>
  condition ? wrapper(children) : children
