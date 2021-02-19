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

export enum BusEvent {
  MSG_ACTIVE_PANEL = "active.panel",
  MSG_EDITOR_EXECUTE = "editor.execute",
  MSG_EDITOR_EXECUTE_ALT = "editor.execute.alt",
  MSG_EDITOR_FOCUS = "editor.focus",
  MSG_EDITOR_SET = "editor.set",
  MSG_QUERY_CANCEL = "query.in.cancel",
  MSG_QUERY_DATASET = "query.out.dataset",
  MSG_QUERY_ERROR = "query.out.error",
  MSG_QUERY_EXEC = "query.in.exec",
  MSG_QUERY_EXPORT = "query.in.export",
  MSG_QUERY_FIND_N_EXEC = "query.build.execute",
  MSG_QUERY_OK = "query.out.ok",
  MSG_QUERY_RUNNING = "query.out.running",
  REACT_READY = "react.ready",
}

export enum ModalId {
  POWER_USER = "POWER_USER",
}

export enum TelemetryTable {
  MAIN = "telemetry",
  CONFIG = "telemetry_config",
}

const BASE = process.env.NODE_ENV === "production" ? "fara" : "alurin"

export const API = `https://${BASE}.questdb.io`
