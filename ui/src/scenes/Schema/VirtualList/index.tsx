import React, { useCallback, useLayoutEffect, useRef, useState } from "react"

type Props<DataType> = {
  items: DataType[]
  itemRenderer: (item: DataType) => React.ReactNode
  rowHeight?: number
}

const VirtualList = <DataType,>({
  items,
  itemRenderer,
  rowHeight = 30,
}: Props<DataType>) => {
  const elementRef = useRef<HTMLDivElement>(null)
  const [visibleRange, setVisibleRange] = useState<[number, number]>([0, 0])
  const [listViewportHeight, setListViewportHeight] = useState(0)

  const updateVisibleRange = useCallback(
    (offset: number) => {
      const firstElementIndex = Math.floor(offset / rowHeight)
      setVisibleRange([
        firstElementIndex,
        firstElementIndex + listViewportHeight / rowHeight,
      ])
    },
    [listViewportHeight, rowHeight],
  )

  useLayoutEffect(() => {
    if (elementRef.current) {
      setListViewportHeight(elementRef.current.offsetHeight)
      updateVisibleRange(0)
    }
  }, [elementRef, updateVisibleRange])

  const handleScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      updateVisibleRange(e.currentTarget.scrollTop)
    },
    [updateVisibleRange],
  )

  return (
    <div
      onScroll={handleScroll}
      ref={elementRef}
      style={{ height: "100%", overflowY: "scroll" }}
    >
      <div style={{ position: "relative" }}>
        <div
          style={{
            height: listViewportHeight,
            width: "100%",
            position: "absolute",
            top: visibleRange[0] * rowHeight,
          }}
        >
          {items.slice(visibleRange[0], visibleRange[1]).map(itemRenderer)}
        </div>
      </div>
    </div>
  )
}

export default VirtualList
