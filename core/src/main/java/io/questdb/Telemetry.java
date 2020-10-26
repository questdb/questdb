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

package io.questdb;

public final class Telemetry {

    // These event types are in addition to those declared in
    // io.questdb.griffin.CompiledQuery. We use them to identify start/stop events of the server

    public static final short SYSTEM_EVENT_UP = 100;
    public static final short SYSTEM_EVENT_DOWN = 101;
    public static final short SYSTEM_ILP_RESERVE_WRITER = 102;

    public static final short ORIGIN_INTERNAL = 1;
    public static final short ORIGIN_HTTP_JSON = 2;
    public static final short ORIGIN_POSTGRES = 3;
    public static final short ORIGIN_HTTP_TEXT = 4;
    public static final short ORIGIN_ILP_TCP = 5;
}
