import { CSSProperties, useEffect } from "react"

export const usePopperStyles = (
  element: HTMLElement,
  styles: CSSProperties,
) => {
  useEffect(() => {
    const css = Object.entries(styles).reduce(
      (acc, [prop, value]) => `${acc} ${prop}: ${value as string};`,
      "z-index: 100;",
    )
    element.setAttribute("style", css)
  }, [element, styles])
}
