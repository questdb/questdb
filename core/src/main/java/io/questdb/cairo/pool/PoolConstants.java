/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.pool;

public final class PoolConstants {
    public static final int CR_DISTRESSED = 5;
    public static final int CR_DROPPED = 6;
    public static final int CR_IDLE = 3;
    public static final int CR_NAME_LOCK = 2;
    public static final int CR_POOL_CLOSE = 1;
    public static final int CR_REOPEN = 4;

    public static String closeReasonText(int reason) {
        return switch (reason) {
            case CR_POOL_CLOSE -> "POOL_CLOSED";
            case CR_NAME_LOCK -> "LOCKED";
            case CR_IDLE -> "IDLE";
            case CR_REOPEN -> "REOPEN";
            case CR_DISTRESSED -> "DISTRESSED";
            case CR_DROPPED -> "DROPPED";
            default -> "UNKNOWN";
        };
    }
}
