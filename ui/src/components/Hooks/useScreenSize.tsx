import React, {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react"
import { shallowEqual } from "react-redux"
import { throttle } from "throttle-debounce"

type Props = Readonly<{
  children: ReactNode
}>

type ScreenSize = Readonly<{
  sm: boolean
}>

enum Breakpoint {
  SM = 767,
}

const ScreenSizeContext = createContext<ScreenSize>({
  sm: window.innerWidth <= Breakpoint.SM,
})

export const ScreenSizeProvider = ({ children }: Props) => {
  const [screenSize, setScreenSize] = useState<ScreenSize>({
    sm: window.innerWidth <= Breakpoint.SM,
  })
  const [_screenSize, _setScreenSize] = useState<ScreenSize | undefined>()

  useEffect(() => {
    const handleResize = throttle(16, () => {
      _setScreenSize({
        sm: window.innerWidth <= Breakpoint.SM,
      })
    })
    window.addEventListener("resize", handleResize)

    return () => {
      window.removeEventListener("resize", handleResize)
    }
  }, [])

  useEffect(() => {
    if (_screenSize && !shallowEqual(_screenSize, screenSize)) {
      setScreenSize(_screenSize)
    }
  }, [screenSize, _screenSize])

  return (
    <ScreenSizeContext.Provider value={screenSize}>
      {children}
    </ScreenSizeContext.Provider>
  )
}

export const useScreenSize = () => useContext(ScreenSizeContext)
