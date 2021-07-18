import React, { useRef, useState } from "react"
import { throttle } from "throttle-debounce"

type Props = Readonly<{
  children: JSX.Element[]
}>

const LazyScroller = ({ children }: Props) => {
  const wrapperEl = useRef<HTMLDivElement | null>(null)
  const singleItemHeight = wrapperEl.current?.children[1]?.getBoundingClientRect()
    ?.height
  const parentEl = wrapperEl.current?.parentElement
  const entireListHeight =
    singleItemHeight && singleItemHeight * children.length
  const viewportHeight = parentEl?.getBoundingClientRect().height
  const numItemsVisibleInViewport =
    viewportHeight &&
    singleItemHeight &&
    Math.ceil(viewportHeight / singleItemHeight)
  const wrapperHeight = singleItemHeight && singleItemHeight * children.length
  const viewportScrollAmount =
    wrapperHeight && parentEl && parentEl.scrollTop / wrapperHeight

  const startingViewableIdx =
    viewportScrollAmount && Math.ceil(viewportScrollAmount * children.length)

  let viewableSubSection = [children[0]]
  if (
    (!!startingViewableIdx || startingViewableIdx === 0) &&
    numItemsVisibleInViewport
  ) {
    const actualStart = Math.max(
      startingViewableIdx - numItemsVisibleInViewport,
      0,
    )
    const actualEnd = startingViewableIdx + 2 * numItemsVisibleInViewport
    viewableSubSection = children.slice(actualStart, actualEnd)
  }

  const [, setState] = useState({})
  function forceUpdate() {
    setState({})
  }
  let spacerStyle = {}
  if (parentEl && viewportHeight) {
    parentEl.onscroll = throttle(60, true, forceUpdate)
    spacerStyle = { height: `${parentEl?.scrollTop - viewportHeight}px` }
  }
  let wrapperStyle = {}
  if (entireListHeight) {
    wrapperStyle = { minHeight: "100%", height: `${entireListHeight}px` }
  }

  return (
    <div ref={wrapperEl} style={wrapperStyle}>
      <div style={spacerStyle} /> {viewableSubSection}
    </div>
  )
}

export default LazyScroller
