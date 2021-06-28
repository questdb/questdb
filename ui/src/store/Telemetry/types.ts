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

export type TelemetryConfigShape = Readonly<{
  enabled: string
  id: string
  version: string
  os: string
  package: string
}>

export type TelemetryRemoteConfigShape = Readonly<{
  cta: boolean
  lastUpdated?: string
}>

export type TelemetryStateShape = Readonly<{
  config?: TelemetryConfigShape
  remoteConfig?: TelemetryRemoteConfigShape
}>

export enum TelemetryAT {
  SET_CONFIG = "TELEMETRY/SET_CONFIG",
  SET_REMOTE_CONFIG = "TELEMETRY/SET_REMOTE_CONFIG",
}

export type SetTelemetryConfigAction = Readonly<{
  payload: TelemetryConfigShape
  type: TelemetryAT.SET_CONFIG
}>

export type SetTelemetryRemoteConfigAction = Readonly<{
  payload: Partial<TelemetryRemoteConfigShape>
  type: TelemetryAT.SET_REMOTE_CONFIG
}>

export type TelemetryAction =
  | SetTelemetryConfigAction
  | SetTelemetryRemoteConfigAction
