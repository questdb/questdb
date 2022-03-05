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

import { Observable, of } from "rxjs"
import { fromFetch as rxFromFetch } from "rxjs/fetch"
import { catchError, map, switchMap } from "rxjs/operators"

type ErrorShape = Readonly<{
  error: true
  message: string
}>

type SuccessShape<T> = Readonly<{
  data: T
  error: false
}>

export const fromFetch = <T extends Record<string, any>>(
  uri: string,
  init: RequestInit = {},
  self = true,
): Observable<SuccessShape<T> | ErrorShape> => {
  const separator = uri[0] === "/" ? "" : "/"
  const url = self ? `${window.location.origin}${separator}${uri}` : uri

  return rxFromFetch(url, init).pipe(
    switchMap((response) => {
      if (response.ok) {
        if (
          response.headers.get("content-type")?.startsWith("application/json")
        ) {
          return response.json()
        }

        return Promise.resolve({})
      }

      // Server is returning a status requiring the client to try something else
      return of({
        error: true,
        message: response.statusText,
      })
    }),
    catchError((error: Error) => {
      // Network or other error, handle appropriately
      console.error(error)
      return of({ error: true, message: error.message })
    }),
    map((response: ErrorShape | T) => {
      if (!response.error) {
        return { data: response as T, error: false }
      }

      return response as ErrorShape
    }),
  )
}
