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

import { ModalId } from "consts"

export type Query = {
  name?: string
  value: string
}

export type QueryGroup = {
  title?: string
  description?: string
  queries: Query[]
}

export type ConsoleConfigShape = Readonly<{
  githubBanner: boolean
  readOnly?: boolean
  savedQueries: Array<Query | QueryGroup>
}>

export type ConsoleStateShape = Readonly<{
  config?: ConsoleConfigShape
  modalId?: ModalId
  sideMenuOpened: boolean
}>

export enum ConsoleAT {
  BOOTSTRAP = "CONSOLE/BOOTSTRAP",
  REFRESH_AUTH_TOKEN = "CONSOLE/REFRESH_AUTH_TOKEN",
  SET_CONFIG = "CONSOLE/SET_CONFIG",
  SET_MODAL_ID = "CONSOLE/SET_MODAL_ID",
  TOGGLE_SIDE_MENU = "CONSOLE/TOGGLE_SIDE_MENU",
}

export type BootstrapAction = Readonly<{
  type: ConsoleAT.BOOTSTRAP
}>

export type RefreshAuthTokenAction = Readonly<{
  payload: boolean
  type: ConsoleAT.REFRESH_AUTH_TOKEN
}>

type SetConfigAction = Readonly<{
  payload: ConsoleConfigShape
  type: ConsoleAT.SET_CONFIG
}>

type SetModalId = Readonly<{
  payload?: ModalId
  type: ConsoleAT.SET_MODAL_ID
}>

type ToggleSideMenuAction = Readonly<{
  type: ConsoleAT.TOGGLE_SIDE_MENU
}>

export type ConsoleAction =
  | BootstrapAction
  | RefreshAuthTokenAction
  | SetConfigAction
  | SetModalId
  | ToggleSideMenuAction
