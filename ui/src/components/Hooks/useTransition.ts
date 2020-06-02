import { MutableRefObject, useEffect } from "react"

import { TransitionDuration } from "../Transition"

export const useTransition = (
  element: HTMLElement,
  _active: boolean,
  timeoutId: MutableRefObject<number | undefined>,
) => {
  useEffect(() => {
    clearTimeout(timeoutId.current)

    if (_active && !document.body.contains(element)) {
      document.body.appendChild(element)
    }

    if (!_active) {
      timeoutId.current = setTimeout(() => {
        document.body.contains(element) && document.body.removeChild(element)
      }, TransitionDuration.REG)
    }
  }, [_active, element, timeoutId])
}
