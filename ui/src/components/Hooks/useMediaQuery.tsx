import React, { createContext, ReactNode, useContext, useMemo } from "react"
import useMedia from "use-media"

type Props = Readonly<{
  children: ReactNode
}>

const mediaQueries = {
  smScreen: "(max-width: 767px)",
}

const MediaQueryContext = createContext({
  smScreen: false,
})

export const MediaQueryProvider = ({ children }: Props) => {
  const smScreen = useMedia(mediaQueries.smScreen)
  const value = useMemo(() => ({ smScreen }), [smScreen])

  return (
    <MediaQueryContext.Provider value={value}>
      {children}
    </MediaQueryContext.Provider>
  )
}

export const useMediaQuery = () => useContext(MediaQueryContext)
