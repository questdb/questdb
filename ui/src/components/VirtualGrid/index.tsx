import React from "react"
import { VirtuosoGrid, VirtuosoGridProps } from "react-virtuoso"

export const VirtualGrid = ({ height, ...rest }: VirtuosoGridProps) => {
  return <VirtuosoGrid style={height ? { height } : {}} {...rest} />
}
