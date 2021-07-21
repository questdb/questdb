import React, { useRef, useState, useEffect } from "react"
import { throttle } from "throttle-debounce"

type Props = Readonly<{
  children: JSX.Element[]
}>

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

  const bottomOfViewport =
    viewportClientRect &&
    parentEl &&
    parentEl.scrollTop + viewportClientRect.height

  const actualHeightOfList = innerWrapClientRect?.height
  const itemsPerPage = 10
  const itemsToRender = itemsPerPage * 3
  const projectedHeightOfList =
    singleItemHeight && itemsToRender * singleItemHeight
  const wrapperHeight = singleItemHeight && singleItemHeight * children.length
  let heightRealDiff = 0
  // TODO possibly makes a bug when you have a DB with only 1 table??
  if (actualHeightOfList && projectedHeightOfList) {
    heightRealDiff = actualHeightOfList - projectedHeightOfList
  }
  const viewportScrollRatio =
    wrapperHeight &&
    parentEl &&
    (parentEl.scrollTop - heightRealDiff) / wrapperHeight

  const startingViewableIdx =
    viewportScrollRatio && Math.ceil(viewportScrollRatio * children.length)
  let currPage = 0
  let actualStart = 0

  // always show at least one item so we can know how big the elements are
  let viewableSubSection = [children[0]]
  if (!!startingViewableIdx || startingViewableIdx === 0) {
    currPage = Math.floor(startingViewableIdx / itemsPerPage)
    actualStart = Math.max((currPage - 1) * itemsPerPage, 0)
    const actualEnd = actualStart + itemsToRender
    viewableSubSection = children.slice(
      startPageIdx * itemsPerPage,
      endPageIdx * itemsPerPage,
    )
  }
  if (singleItemHeight && innerWrapClientRect && bottomOfViewport) {
    const diffFromBot = innerWrapClientRect.bottom - bottomOfViewport
    if (diffFromBot < -300) {
      // Delay the change so the application actually gets the time to re-render
      setTimeout(() => {
        setEnd(endPageIdx + 1)
      })
    // If we're too far away from the bottom and the height is the same
    } else if (diffFromBot > 500 && heightRealDiff === 0) {
      setTimeout(() => {
        setEnd(endPageIdx - 1)
      })
    }
    console.log(diffFromBot)
    console.log(actualStart)
    console.log("-----------")
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
