import React, { createContext, PropsWithChildren } from "react"
import * as QuestDB from "utils/questdb"

const questClient = new QuestDB.Client()

type Props = {}

type ContextProps = {
  quest: QuestDB.Client
}

const defaultValues = {
  quest: questClient,
}

export const QuestContext = createContext<ContextProps>(defaultValues)

export const QuestProvider = ({ children }: PropsWithChildren<Props>) => {
  return (
    <QuestContext.Provider
      value={{
        quest: questClient,
      }}
    >
      {children}
    </QuestContext.Provider>
  )
}
