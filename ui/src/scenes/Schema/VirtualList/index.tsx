import React, { useCallback, useLayoutEffect, useRef, useState } from "react"

type Props<DataType> = {
  items: DataType[]
  itemRenderer: (item: DataType) => React.ReactNode
}

const VirtualList = <DataType,>({ items, itemRenderer }: Props<DataType>) => {
  const elementRef = useRef<HTMLDivElement>(null)
  const [visibleRange, setVisibleRange] = useState<[number, number]>([0, 0])
  const [listViewportHeight, setListViewportHeight] = useState(0)

  const updateFirstVisibleElement = useCallback(
    (index: number) => {
      setVisibleRange([index, index + listViewportHeight / 30])
    },
    [listViewportHeight],
  )

  useLayoutEffect(() => {
    if (elementRef.current) {
      setListViewportHeight(elementRef.current.offsetHeight)
      updateFirstVisibleElement(0)
    }
  }, [elementRef, updateFirstVisibleElement])

  const handleScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      const firstElementIndex = Math.floor(e.currentTarget.scrollTop / 30)
      updateFirstVisibleElement(firstElementIndex)
    },
    [updateFirstVisibleElement],
  )

  return (
    <div
      onScroll={handleScroll}
      ref={elementRef}
      style={{ height: "100%", overflowY: "scroll" }}
    >
      <div style={{ height: 30 * items.length, position: "relative" }}>
        <div
          style={{
            height: listViewportHeight,
            width: "100%",
            position: "absolute",
            top: visibleRange[0] * 30,
          }}
        >
          {items.slice(visibleRange[0], visibleRange[1]).map(itemRenderer)}
        </div>
      </div>
    </div>
  )
}

export default VirtualList
