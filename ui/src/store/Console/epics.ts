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

import { Epic, ofType } from "redux-observable"
import { filter, map, switchMap, switchMapTo, tap } from "rxjs/operators"
import { NEVER, of, timer } from "rxjs"

import { actions } from "store"
import {
  BootstrapAction,
  ConsoleConfigShape,
  ConsoleAction,
  ConsoleAT,
  RefreshAuthTokenAction,
  StoreAction,
  StoreShape,
} from "types"
import { fromFetch } from "utils"
import { getValue, setValue } from "utils/localStorage"
import { StoreKey } from "utils/localStorage/types"

type AuthPayload = Readonly<
  Partial<{
    expiry: number
    refreshRoute: string
  }>
>

export const getConfig: Epic<StoreAction, ConsoleAction, StoreShape> = (
  action$,
) =>
  action$.pipe(
    ofType<StoreAction, BootstrapAction>(ConsoleAT.BOOTSTRAP),
    switchMap(() =>
      fromFetch<ConsoleConfigShape>("assets/console-configuration.json").pipe(
        map((response) => {
          if (!response.error) {
            return actions.console.setConfig(response.data)
          }
        }),
        filter((a): a is ConsoleAction => !!a),
      ),
    ),
  )

export const triggerRefreshTokenOnBootstrap: Epic<
  StoreAction,
  ConsoleAction,
  StoreShape
> = (action$) =>
  action$.pipe(
    ofType<StoreAction, BootstrapAction>(ConsoleAT.BOOTSTRAP),
    switchMap(() => {
      const authPayload = getValue(StoreKey.AUTH_PAYLOAD)

      if (authPayload != null) {
        return of(actions.console.refreshAuthToken(true))
      }

      return NEVER
    }),
  )

export const refreshToken: Epic<StoreAction, ConsoleAction, StoreShape> = (
  action$,
) =>
  action$.pipe(
    ofType<StoreAction, RefreshAuthTokenAction>(ConsoleAT.REFRESH_AUTH_TOKEN),
    switchMap((action) => {
      const authPayload = getValue(StoreKey.AUTH_PAYLOAD)

      if (authPayload != null) {
        try {
          const { expiry, refreshRoute } = JSON.parse(
            authPayload,
          ) as AuthPayload

          if (refreshRoute == null) {
            return NEVER
          }

          const fetch$ = fromFetch<AuthPayload>(refreshRoute).pipe(
            tap((response) => {
              if (!response.error) {
                setValue(StoreKey.AUTH_PAYLOAD, JSON.stringify(response.data))
              }
            }),
            switchMap(() => of(actions.console.refreshAuthToken(false))),
          )

          const offset = 30e3
          const waitUntil = (expiry ?? 0) * 1e3 - offset - Date.now()

          if (waitUntil > -offset) {
            return timer(Math.max(0, waitUntil)).pipe(switchMapTo(fetch$))
          }

          if (action.payload) {
            return fetch$
          }

          return NEVER
        } catch (error) {
          return NEVER
        }
      }

      return NEVER
    }),
  )

export default [getConfig, triggerRefreshTokenOnBootstrap, refreshToken]
