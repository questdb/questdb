import React, {
  MouseEvent as ReactMouseEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react"
import styled from "styled-components"
import { DragIndicator } from "@styled-icons/material/DragIndicator"

import { color } from "utils"

type Props = Readonly<{
  max?: number
  min?: number
  onChange: (x: number) => void
}>

const DragIcon = styled(DragIndicator)`
  position: absolute;
  color: ${color("gray1")};
`

const Wrapper = styled.div`
  display: flex;
  width: 8px;
  height: 100%;
  align-items: center;
  justify-content: center;
  background: ${color("draculaBackgroundDarker")};
  border: 1px solid rgba(255, 255, 255, 0.03);
  border-top: none;
  border-bottom: none;

  &:hover {
    background: ${color("draculaSelection")};
    cursor: ew-resize;
  }
`

const Ghost = styled(Wrapper)`
  position: absolute;
  width: 8px;
  top: 0;
  bottom: 0;
  z-index: 20;

  &:hover {
    background: ${color("draculaPurple")};
    cursor: ew-resize;
  }
`

type Position = Readonly<{
  x: number
}>

export const Splitter = ({ max, min, onChange }: Props) => {
  const [pressed, setPressed] = useState(false)
  const [left, setLeft] = useState<number>(0)
  const [xOffset, setXOffset] = useState<number>(0)
  const splitter = useRef<HTMLDivElement | null>(null)
  const [position, setPosition] = useState<Position>({ x: 0 })

  const handleMouseMove = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation()
      event.preventDefault()

      if (
        (min &&
          max &&
          event.clientX > min &&
          event.clientX < window.outerWidth - max) ||
        (!min && max && event.clientX < window.outerWidth - max) ||
        (!max && min && event.clientX > min) ||
        (!min && !max)
      ) {
        setPosition({
          x: event.clientX,
        })
      }
    },
    [max, min],
  )

  const handleMouseUp = useCallback(() => {
    document.removeEventListener("mouseup", handleMouseUp)
    document.removeEventListener("mousemove", handleMouseMove)
    setPressed(false)
  }, [handleMouseMove])

  const handleMouseDown = useCallback(
    (event: ReactMouseEvent<HTMLDivElement>) => {
      if (splitter.current && splitter.current.parentElement) {
        const { x } = splitter.current.parentElement.getBoundingClientRect()

        setLeft(event.clientX - x)
        setXOffset(x)
        setPressed(true)

        document.addEventListener("mouseup", handleMouseUp)
        document.addEventListener("mousemove", handleMouseMove)
      }
    },
    [handleMouseMove, handleMouseUp],
  )

  useEffect(() => {
    if (!pressed && position.x) {
      onChange(position.x - left - xOffset)
      setLeft(0)
      setPosition({ x: 0 })
    }
  }, [onChange, position, pressed, left, xOffset])

  return (
    <>
      <Wrapper onMouseDown={handleMouseDown} ref={splitter}>
        <DragIcon size="12px" />
      </Wrapper>

      {position.x > 0 && (
        <Ghost
          style={{
            left: `${position.x - xOffset}px`,
          }}
        />
      )}
    </>
  )
}
