import React, { useRef, useState } from "react"
import styled from "styled-components"
import * as QuestDB from "utils/questdb"

type Props = Readonly<{
  children: JSX.Element[]
}>

const LazyScroller = ({ children }: Props) => {
  const wrapperEl = useRef<HTMLDivElement | null>(null)
  const singleItemHeight = wrapperEl.current?.children[1]?.getBoundingClientRect()
    ?.height
  const parentEl = wrapperEl.current?.parentElement
  const viewportHeight = singleItemHeight && singleItemHeight * children.length
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
    console.log(singleItemHeight)
    console.log(viewportHeight)
    viewableSubSection = children.slice(
      startingViewableIdx,
      startingViewableIdx + numItemsVisibleInViewport,
    )
  }

  const [, setState] = useState({})
  function forceUpdate() {
    console.log("trying to update")
    setState({})
  }
  let spacerStyle = {}
  if (parentEl) {
    parentEl.onscroll = forceUpdate
    spacerStyle = { height: `${parentEl?.scrollTop}px` }
  }
  let wrapperStyle = {}
  if (viewportHeight) {
    wrapperStyle = { "min-height": "100%", height: `${viewportHeight}px` }
  }

  return (
    <div ref={wrapperEl} style={wrapperStyle}>
      <div style={spacerStyle} /> {viewableSubSection}
    </div>
  )
}

export default LazyScroller
