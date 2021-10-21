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
