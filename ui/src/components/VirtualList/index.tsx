import React from "react"
import { Virtuoso, VirtuosoProps } from "react-virtuoso"

export const VirtualList = ({ height, ...rest }: VirtuosoProps<any>) => {
  return <Virtuoso style={height ? { height } : {}} {...rest} />
}
