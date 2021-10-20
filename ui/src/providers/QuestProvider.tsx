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
