/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import React, { ChangeEvent, useCallback, useState } from "react"

import { Input } from "components"

type Props = Readonly<{
  dirty: boolean
  onInit: () => void
}>

const EmailInput = ({ dirty, onInit }: Props) => {
  const [email, setEmail] = useState("")

  const handleChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setEmail(event.target?.value)

      if (!dirty) {
        onInit()
      }
    },
    [dirty, onInit],
  )

  return (
    <Input
      name="email"
      onChange={handleChange}
      pattern={
        "^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$"
      }
      required
      size="lg"
      title="email"
      type="email"
      value={email}
    />
  )
}

export default EmailInput
