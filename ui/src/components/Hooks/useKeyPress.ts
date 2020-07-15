import { useEffect, useState } from "react"

export const useKeyPress = (
  targetKey: string,
  options?: { preventDefault: boolean },
) => {
  const [keyPressed, setKeyPressed] = useState(false)

  const downHandler = (event: KeyboardEvent) => {
    if (event.key === targetKey) {
      if (options?.preventDefault) {
        event.preventDefault()
      }

      setKeyPressed(true)
    }
  }

  const upHandler = (event: KeyboardEvent) => {
    if (event.key === targetKey) {
      if (options?.preventDefault) {
        event.preventDefault()
      }

      setKeyPressed(false)
    }
  }

  useEffect(() => {
    window.addEventListener("keydown", downHandler)
    window.addEventListener("keyup", upHandler)

    return () => {
      window.removeEventListener("keydown", downHandler)
      window.removeEventListener("keyup", upHandler)
    }
  })

  return keyPressed
}
