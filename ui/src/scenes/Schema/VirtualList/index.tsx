import React, { useLayoutEffect, useRef, useState } from "react"

type Props<DataType> = {
  items: DataType[]
  itemRenderer: (item: DataType) => React.ReactNode
}

const VirtualList = <DataType,>({ items, itemRenderer }: Props<DataType>) => {
  const elementRef = useRef<HTMLDivElement>(null)
  const [visibleRange, setVisibleRange] = useState<[number, number]>([0, 0])
  const [listViewportHeight, setListViewportHeight] = useState(0)

  useLayoutEffect(() => {
    if (elementRef.current) {
      setListViewportHeight(elementRef.current.offsetHeight)
    }
  }, [elementRef])

  function handleScroll(e: React.UIEvent<HTMLDivElement>) {
    const firstElementIndex = Math.floor(e.currentTarget.scrollTop / 30)
    const lastElementIndex = Math.floor(
      (e.currentTarget.scrollTop + listViewportHeight) / 30,
    )
    setVisibleRange([firstElementIndex, lastElementIndex])
  }

  console.log(listViewportHeight)

  return (
    <div
      onScroll={handleScroll}
      ref={elementRef}
      style={{ height: "100%", overflowY: "scroll" }}
    >
      <div style={{ height: 30 * items.length, position: "relative" }}>
        <div
          style={{
            height: listViewportHeight * 3,
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
