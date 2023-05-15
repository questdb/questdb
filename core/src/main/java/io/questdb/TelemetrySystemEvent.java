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
    // CPU classes: -20 - 1-4 cores, -21 - 5-8 cores, -22 - 9-16 cores, -23 - 17-32 cores, -24 - 33-64 cores, -25 - 65+ cores
    public static final short SYSTEM_CPU_CLASS_BASE = -20;
    // DB size classes: -30 - <10GB, -31 - (10GB,50GB], -32 - (50GB,100GB], -33 - (100GB,500GB], -34 - (500GB,1TB], -35 - (1TB,5TB], -36 - (5TB,10TB], -37 - >10TB
    public static final short SYSTEM_DB_SIZE_CLASS_BASE = -30;
    public static final short SYSTEM_DOWN = 101;
    // OS classes: -10 - Linux, -11 - OS X, -12 - Windows, -13 - BSD
    public static final short SYSTEM_OS_CLASS_BASE = -10;
    // Table count classes: -40 - 0-10 tables, -41 - 11-25 tables, -42 - 26-50 tables, -43 - 51-100 tables, -44 - 101-250 tables, -45 - 251-1000 tables, -46 - 1001+ tables
    public static final short SYSTEM_TABLE_COUNT_CLASS_BASE = -40;
    public static final short SYSTEM_UP = 100;
    public static final short WAL_APPLY_RESUME = 108;
    public static final short WAL_APPLY_SUSPEND = 107;
    public static final short WAL_TXN_APPLY_START = 103;
    public static final short WAL_TXN_DATA_APPLIED = 105;
    public static final short WAL_TXN_SQL_APPLIED = 106;
    public static final short WAL_TXN_STRUCTURE_CHANGE_APPLIED = 104;
}
