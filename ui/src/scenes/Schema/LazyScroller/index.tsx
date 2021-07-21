import React, { useRef, useState } from "react"
import { throttle } from "throttle-debounce"

type Props = Readonly<{
  children: JSX.Element[]
}>
const ITEMS_PER_PAGE = 10

const LazyScroller = ({ children }: Props) => {
  const wrapperEl = useRef<HTMLDivElement | null>(null)
  const innerEl = useRef<HTMLDivElement | null>(null)
  const [startPageIdx, setStart] = useState(0)
  const [endPageIdx, setEnd] = useState(2)
  // FIXME if the first element is expanded it will mess up the calculations, we should pick 3 and take the less of them
  const singleItemHeight = innerEl.current?.children[0]?.getBoundingClientRect()
    ?.height
  const parentEl = wrapperEl.current?.parentElement
  const viewportClientRect = parentEl?.getBoundingClientRect()
  const innerWrapClientRect = innerEl.current?.getBoundingClientRect()

  const topOfViewport = parentEl?.scrollTop
  const bottomOfViewport =
    viewportClientRect &&
    parentEl &&
    parentEl.scrollTop + viewportClientRect.height

  const actualHeightOfList = innerWrapClientRect?.height
  const itemsToRender = ITEMS_PER_PAGE * (endPageIdx - startPageIdx)
  const projectedHeightOfList =
    singleItemHeight && itemsToRender * singleItemHeight
  let heightRealDiff = 0
  if (actualHeightOfList && projectedHeightOfList) {
    heightRealDiff = actualHeightOfList - projectedHeightOfList
  }

  const actualStart = startPageIdx * ITEMS_PER_PAGE

  // always show at least one item so we can know how big the elements are
  let viewableSubSection = [children[0]]
  if (singleItemHeight) {
    viewableSubSection = children.slice(
      actualStart,
      endPageIdx * ITEMS_PER_PAGE,
    )
  }
  if (innerWrapClientRect && bottomOfViewport && topOfViewport) {
    const diffFromBot = innerWrapClientRect.bottom - bottomOfViewport
    const diffFromTop = innerWrapClientRect.top
    if (diffFromTop < -300 && heightRealDiff === 0) {
      setTimeout(() => {
        setStart(startPageIdx + 1)
      })
    } else if (diffFromTop > 50 && startPageIdx > 0) {
      setTimeout(() => {
        setStart(startPageIdx - 1)
      })
    }

    if (diffFromBot < -300) {
      // Delay the change so the application actually gets the time to re-render
      setTimeout(() => {
        setEnd(endPageIdx + 1)
      })
    } else if (diffFromBot > 500 && heightRealDiff === 0) {
      // If we're too far away from the bottom and the height is the same
      setTimeout(() => {
        setEnd(endPageIdx - 1)
      })
    }
  }

  const [, setState] = useState({})
  function forceUpdate() {
    setState({})
  }
  let spacerStyle = { height: "0px" }
  if (singleItemHeight && parentEl) {
    parentEl.onscroll = throttle(60, true, forceUpdate)
    spacerStyle = {
      height: `${actualStart * singleItemHeight}px`,
    }
  }
  const wrapperHeight =
    singleItemHeight &&
    viewportClientRect &&
    Math.max(singleItemHeight * children.length, viewportClientRect.height)
  let wrapperStyle = {}
  if (wrapperHeight) {
    wrapperStyle = { minHeight: `${wrapperHeight}px` }
  }

  return (
    <div ref={wrapperEl} style={wrapperStyle}>
      <div style={spacerStyle} />
      <div ref={innerEl}>{viewableSubSection}</div>
    </div>
  )
}

export default LazyScroller
