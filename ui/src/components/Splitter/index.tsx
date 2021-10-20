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

import React, {
  Children,
  MouseEvent as ReactMouseEvent,
  TouchEvent as ReactTouchEvent,
  ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react"
import styled, { createGlobalStyle, css } from "styled-components"
import { DragIndicator } from "@styled-icons/material/DragIndicator"

import { color } from "utils"

const PreventUserSelectionHorizontal = createGlobalStyle`
  html {
    user-select: none;
    cursor: ew-resize !important;
    pointer-events: none;
  }
`

const PreventUserSelectionVertical = createGlobalStyle`
  html {
    user-select: none;
    cursor: row-resize !important;
    pointer-events: none;
  }
`

type Props = Readonly<{
  children: ReactNode
  direction: "vertical" | "horizontal"
  fallback: number
  max?: number
  min?: number
  onChange?: (value: number) => void
}>

const HorizontalDragIcon = styled(DragIndicator)`
  position: absolute;
`

const VerticalDragIcon = styled(HorizontalDragIcon)`
  transform: rotate(90deg);
`

const wrapperStyles = css`
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(0, 0, 0, 0.1);
  background: ${color("draculaBackgroundDarker")};
  color: ${color("gray1")};

  &:hover {
    background: ${color("draculaSelection")};
    color: ${color("draculaForeground")};
  }
`

const HorizontalWrapper = styled.div`
  ${wrapperStyles};
  width: 1rem;
  height: 100%;
  border-top: none;
  border-bottom: none;
  cursor: ew-resize;
`

const VerticalWrapper = styled.div`
  ${wrapperStyles};
  width: 100%;
  height: 1rem;
  border-left: none;
  border-right: none;
  cursor: row-resize;
`

const ghostStyles = css`
  position: absolute;
  z-index: 20;
  background: ${color("draculaPurple")};

  &:hover {
    background: ${color("draculaPurple")};
  }
`

const HorizontalGhost = styled.div`
  ${ghostStyles};
  width: 1rem;
  top: 0;
  bottom: 0;
`

const VerticalGhost = styled.div`
  ${ghostStyles};
  height: 1rem;
  left: 0;
  right: 0;
`

export const Splitter = ({
  children: rawChildren,
  fallback,
  direction,
  max,
  min,
  onChange,
}: Props) => {
  const [offset, setOffset] = useState(0)
  const [originalPosition, setOriginalPosition] = useState(0)
  const [ghostPosition, setGhostPosition] = useState(0)
  const [pressed, setPressed] = useState(false)
  const [basis, setBasis] = useState<number>()
  const splitter = useRef<HTMLDivElement | null>(null)
  const firstChild = useRef<HTMLDivElement | null>(null)

  const children = Children.toArray(rawChildren)

  const handleMouseMove = useCallback(
    (event: TouchEvent | MouseEvent) => {
      event.stopPropagation()
      const clientPosition = direction === "horizontal" ? "clientX" : "clientY"
      const side = direction === "horizontal" ? "outerWidth" : "outerHeight"
      let position = 0

      if (window.TouchEvent && event instanceof TouchEvent) {
        position = event.touches[0][clientPosition]
      }

      if (event instanceof MouseEvent) {
        position = event[clientPosition]
      }

      if (
        (min != null &&
          max != null &&
          position > min &&
          position < window[side] - max) ||
        (min == null && max != null && position < window[side] - max) ||
        (max == null && min != null && position > min) ||
        (min == null && max == null)
      ) {
        setGhostPosition(position)
      }
    },
    [direction, max, min],
  )

  const handleMouseUp = useCallback(() => {
    document.removeEventListener("mouseup", handleMouseUp)
    document.removeEventListener("mousemove", handleMouseMove)
    document.removeEventListener("touchend", handleMouseUp)
    document.removeEventListener("touchmove", handleMouseMove)
    setPressed(false)
  }, [handleMouseMove])

  const handleMouseDown = useCallback(
    (event: ReactTouchEvent | ReactMouseEvent) => {
      if (splitter.current?.parentElement) {
        const clientPosition =
          direction === "horizontal" ? "clientX" : "clientY"
        const coordinate = direction === "horizontal" ? "x" : "y"
        const offset = splitter.current.parentElement.getBoundingClientRect()[
          coordinate
        ]
        let position = 0

        if (window.TouchEvent && event.nativeEvent instanceof TouchEvent) {
          position = event.nativeEvent.touches[0][clientPosition]
        }

        if (event.nativeEvent instanceof MouseEvent) {
          position = event.nativeEvent[clientPosition]
        }

        setOriginalPosition(position)
        setOffset(offset)
        setPressed(true)

        document.addEventListener("mouseup", handleMouseUp)
        document.addEventListener("mousemove", handleMouseMove, {
          passive: true,
        })
        document.addEventListener("touchend", handleMouseUp)
        document.addEventListener("touchmove", handleMouseMove, {
          passive: true,
        })
      }
    },
    [direction, handleMouseMove, handleMouseUp],
  )

  useEffect(() => {
    if (!pressed && ghostPosition && firstChild.current) {
      const measure = direction === "horizontal" ? "width" : "height"

      const size =
        firstChild.current.getBoundingClientRect()[measure] +
        (ghostPosition - originalPosition)

      setOriginalPosition(0)
      setGhostPosition(0)
      setBasis(size)

      if (onChange) {
        onChange(size)
      }
    }
  }, [direction, ghostPosition, onChange, originalPosition, pressed])

  useEffect(() => {
    setBasis(fallback)
  }, [fallback])

  const style = {
    display: "flex",
    flexGrow: 0,
    flexBasis: basis ?? fallback,
    flexShrink: 1,
  }

  if (children.length === 1) {
    return <>{children[0]}</>
  }

  if (direction === "horizontal") {
    return (
      <>
        {React.isValidElement(children[0]) &&
          React.cloneElement(children[0], {
            ref: firstChild,
            style,
          })}

        <HorizontalWrapper
          onMouseDown={handleMouseDown}
          onTouchStart={handleMouseDown}
          ref={splitter}
        >
          <HorizontalDragIcon size="16px" />
        </HorizontalWrapper>

        {children[1]}

        {ghostPosition > 0 && (
          <>
            <HorizontalGhost
              style={{
                left: `${ghostPosition - offset}px`,
              }}
            />
            <PreventUserSelectionHorizontal />
          </>
        )}
      </>
    )
  }

  return (
    <>
      {React.isValidElement(children[0]) &&
        React.cloneElement(children[0], {
          ref: firstChild,
          style,
        })}

      <VerticalWrapper
        onMouseDown={handleMouseDown}
        onTouchStart={handleMouseDown}
        ref={splitter}
      >
        <VerticalDragIcon size="16px" />
      </VerticalWrapper>

      {children[1]}

      {ghostPosition > 0 && (
        <>
          <VerticalGhost
            style={{
              top: `${ghostPosition - offset}px`,
            }}
          />
          <PreventUserSelectionVertical />
        </>
      )}
    </>
  )
}
