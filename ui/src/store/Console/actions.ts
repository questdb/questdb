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

import { ModalId } from "consts"
import { ConsoleConfigShape, ConsoleAction, ConsoleAT } from "types"

const bootstrap = (): ConsoleAction => ({
  type: ConsoleAT.BOOTSTRAP,
})

const refreshAuthToken = (init: boolean): ConsoleAction => ({
  payload: init,
  type: ConsoleAT.REFRESH_AUTH_TOKEN,
})

const setConfig = (payload: ConsoleConfigShape): ConsoleAction => ({
  payload,
  type: ConsoleAT.SET_CONFIG,
})

const setModalId = (payload?: ModalId): ConsoleAction => ({
  payload,
  type: ConsoleAT.SET_MODAL_ID,
})

const toggleSideMenu = (): ConsoleAction => ({
  type: ConsoleAT.TOGGLE_SIDE_MENU,
})

export default {
  bootstrap,
  refreshAuthToken,
  setConfig,
  setModalId,
  toggleSideMenu,
}
