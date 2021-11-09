import React from "react"
import { Virtuoso, VirtuosoProps } from "react-virtuoso"

export type { ListRange } from "react-virtuoso"

export const VirtualList = ({ height, ...rest }: VirtuosoProps<any>) => {
  return <Virtuoso style={height ? { height } : {}} {...rest} />
}
