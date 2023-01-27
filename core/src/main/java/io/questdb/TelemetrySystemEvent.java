/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public final class TelemetrySystemEvent {
    public static final short ILP_RESERVE_WRITER = 102;
    public static final short SYSTEM_DOWN = 101;
    public static final short SYSTEM_UP = 100;
    public static final short WAL_APPLY_RESUME = 108;
    public static final short WAL_APPLY_SUSPEND = 107;
    public static final short WAL_TXN_APPLY_START = 103;
    public static final short WAL_TXN_DATA_APPLIED = 105;
    public static final short WAL_TXN_SQL_APPLIED = 106;
    public static final short WAL_TXN_STRUCTURE_CHANGE_APPLIED = 104;
}
