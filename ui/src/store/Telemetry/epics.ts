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
import { delay, filter, map, switchMap, withLatestFrom } from "rxjs/operators"
import { from, NEVER, of } from "rxjs"

import { API, ModalId, TelemetryTable } from "consts"
import { actions, selectors } from "store"
import {
  BootstrapAction,
  ConsoleAction,
  ConsoleAT,
  SetTelemetryConfigAction,
  SetTelemetryRemoteConfigAction,
  StoreAction,
  StoreShape,
  TelemetryAction,
  TelemetryAT,
  TelemetryConfigShape,
  TelemetryRemoteConfigShape,
} from "types"

import { fromFetch } from "utils"
import * as QuestDB from "utils/questdb"

const quest = new QuestDB.Client()

export const getConfig: Epic<StoreAction, TelemetryAction, StoreShape> = (
  action$,
) =>
  action$.pipe(
    ofType<StoreAction, BootstrapAction>(ConsoleAT.BOOTSTRAP),
    switchMap(() =>
      from(
        quest.query<TelemetryConfigShape>(`${TelemetryTable.CONFIG} limit -1`),
      ),
    ),
    switchMap((response) => {
      if (response.type === QuestDB.Type.DQL) {
        return of(actions.telemetry.setConfig(response.data[0]))
      }

      return NEVER
    }),
  )

export const getRemoteConfig: Epic<StoreAction, TelemetryAction, StoreShape> = (
  action$,
  state$,
) =>
  action$.pipe(
    ofType<StoreAction, SetTelemetryConfigAction>(TelemetryAT.SET_CONFIG),
    withLatestFrom(state$),
    switchMap(([_, state]) => {
      const config = selectors.telemetry.getConfig(state)
      if (config?.enabled) {
        return fromFetch<Partial<TelemetryRemoteConfigShape>>(
          `${API}/config`,
          {
            method: "POST",
            body: JSON.stringify(config),
          },
          false,
        )
      }

      return NEVER
    }),
    switchMap((response) => {
      if (response.error) {
        return NEVER
      }

      return of(actions.telemetry.setRemoteConfig(response.data))
    }),
  )

export const powerUserCta: Epic<StoreAction, ConsoleAction, StoreShape> = (
  action$,
  state$,
) =>
  action$.pipe(
    ofType<StoreAction, SetTelemetryRemoteConfigAction>(
      TelemetryAT.SET_REMOTE_CONFIG,
    ),
    withLatestFrom(state$),
    switchMap(([_, state]) => {
      const config = selectors.telemetry.getRemoteConfig(state)

      if (config?.cta) {
        return of(actions.console.setModalId(ModalId.POWER_USER))
      }

      return NEVER
    }),
  )

export const startTelemetry: Epic<StoreAction, TelemetryAction, StoreShape> = (
  action$,
  state$,
) =>
  action$.pipe(
    ofType<StoreAction, SetTelemetryRemoteConfigAction>(
      TelemetryAT.SET_REMOTE_CONFIG,
    ),
    withLatestFrom(state$),
    switchMap(([_, state]) => {
      const remoteConfig = selectors.telemetry.getRemoteConfig(state)

      if (remoteConfig?.lastUpdated) {
        return from(
          quest.queryRaw(
            `SELECT cast(created as long), event, origin
                FROM ${TelemetryTable.MAIN}
                WHERE created > '${new Date(
                  remoteConfig.lastUpdated,
                ).toISOString()}'
            `,
          ),
        )
      }

      return NEVER
    }),
    withLatestFrom(state$),
    switchMap(([result, state]) => {
      const remoteConfig = selectors.telemetry.getRemoteConfig(state)
      const config = selectors.telemetry.getConfig(state)

      if (
        config?.id != null &&
        result.type === QuestDB.Type.DQL &&
        result.count > 0
      ) {
        return fromFetch<{ _: void }>(
          `${API}/add`,
          {
            method: "POST",
            body: JSON.stringify({
              columns: result.columns,
              dataset: result.dataset,
              id: config.id,
              version: config.version,
              os: config.os,
              package: config.package,
            }),
          },
          false,
        ).pipe(
          map((response) => {
            if (!response.error) {
              const timestamp = result.dataset[result.count - 1][0] as string

              return actions.telemetry.setRemoteConfig({
                ...remoteConfig,
                lastUpdated: timestamp,
              })
            }
          }),
          delay(36e5),
          filter((a): a is TelemetryAction => !!a),
        )
      }

      return NEVER
    }),
  )

export default [getConfig, getRemoteConfig, powerUserCta, startTelemetry]
