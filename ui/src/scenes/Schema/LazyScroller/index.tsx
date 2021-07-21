/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import React, { useRef, useState, useEffect } from "react"
import { throttle } from "throttle-debounce"

type Props = Readonly<{
  children: JSX.Element[]
}>
const ITEMS_PER_PAGE = 10
function getClientHeight(el: Element) {
  return el.getBoundingClientRect().height
}
function getMinHeightFromElList(listOfEls: HTMLCollection): number {
  const elemCount = Math.min(listOfEls.length, 3)
  const arrayOfHeights: number[] = []
  for (let idx = 0; idx < elemCount; idx++) {
    arrayOfHeights.push(getClientHeight(listOfEls[idx]))
  }
  return Math.min.apply(undefined, arrayOfHeights)
}

const LazyScroller = ({ children }: Props) => {
  const wrapperEl = useRef<HTMLDivElement | null>(null)
  const innerEl = useRef<HTMLDivElement | null>(null)
  const [startPageIdx, setStart] = useState(0)
  const [endPageIdx, setEnd] = useState(2)
  // FIXME if the first element is expanded it will mess up the calculations, we should pick 3 and take the less of them
  let singleItemHeight: number = 0
  if (innerEl.current && innerEl.current.children.length >= 1) {
    singleItemHeight = getMinHeightFromElList(innerEl.current.children)
  }
  const parentEl = wrapperEl.current?.parentElement
  let viewportClientHeight = null
  if (parentEl) {
    viewportClientHeight = getClientHeight(parentEl)
  }
  const innerWrapClientRect = innerEl.current?.getBoundingClientRect()

  const bottomOfViewport =
    viewportClientHeight &&
    parentEl &&
    parentEl.scrollTop + viewportClientHeight

  // Calculate if the list is actually taller than it should be i.e. someone has expanded something in the list
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
  useEffect(() => {
    if (innerWrapClientRect && bottomOfViewport) {
      const diffFromBot = innerWrapClientRect.bottom - bottomOfViewport
      const diffFromTop = innerWrapClientRect.top
      if (diffFromTop < -300 && heightRealDiff === 0) {
        setStart(startPageIdx + 1)
      } else if (diffFromTop > 50 && startPageIdx > 0) {
        setStart(startPageIdx - 1)
      }

      if (diffFromBot < -300) {
        setEnd(endPageIdx + 1)
      } else if (diffFromBot > 500 && heightRealDiff === 0) {
        // If we're too far away from the bottom and the height is the same
        setEnd(endPageIdx - 1)
      }
    }
  }, [innerWrapClientRect?.top])

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
    viewportClientHeight &&
    Math.max(singleItemHeight * children.length, viewportClientHeight)
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
