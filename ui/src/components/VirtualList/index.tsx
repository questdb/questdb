import React, { ReactNode } from "react"
import { Virtuoso } from "react-virtuoso"

type Props = Readonly<{
  height?: number
  isScrolling?: (isScrolling: boolean) => void
  itemContent: (index: number, data: any) => ReactNode
  totalCount: number | undefined
}>

export const VirtualList = ({
  height,
  isScrolling,
  itemContent,
  totalCount,
}: Props) => {
  return (
    <Virtuoso
      isScrolling={isScrolling}
      itemContent={itemContent}
      style={height ? { height } : {}}
      totalCount={totalCount}
    />
  )
}
