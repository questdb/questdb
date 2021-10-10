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

import { API } from "consts"

type Error = Readonly<{
  error: true
  message: string
  status?: number
}>

type ResponseShape<T> = Readonly<{
  data?: T
  error: false
}>

export const fetchApi = async <T>(
  path: string,
  init?: RequestInit,
): Promise<ResponseShape<T> | Error> => {
  try {
    const response = await fetch(`${API}/${path}`, init)
    const contentType = response.headers.get("content-type")

    if (!response.ok) {
      return {
        error: true,
        message: response.statusText,
        status: response.status,
      }
    }

    if (contentType === "application/json") {
      const data = (await response.json()) as T
      return {
        data,
        error: false,
      }
    }

    return { error: false }
  } catch (ex) {
    const e = ex as { message: string }
    return {
      error: true,
      message: e.message,
    }
  }
}
