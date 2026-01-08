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

import io.questdb.cairo.TableToken;

@FunctionalInterface
public interface PoolListener {
    short EV_CREATE = 10;
    short EV_CREATE_EX = 14;
    short EV_DROPPED = 27;
    short EV_EXPIRE = 17;
    short EV_EX_RESEND = 21;
    short EV_FULL = 25;
    short EV_GET = 11;
    short EV_LOCK_BUSY = 7;
    short EV_LOCK_CLOSE = 19;
    short EV_LOCK_SUCCESS = 6;
    short EV_NOT_LOCKED = 9;
    short EV_NOT_LOCK_OWNER = 12;
    short EV_OUT_OF_POOL_CLOSE = 2;
    short EV_POOL_CLOSED = 24;
    short EV_POOL_OPEN = 23;
    short EV_REMOVE_TOKEN = 26;
    short EV_RETURN = 1;
    short EV_UNEXPECTED_CLOSE = 3;
    short EV_UNLOCKED = 8;
    byte SRC_READER = 2;
    byte SRC_SEQUENCER_METADATA = 3;
    byte SRC_SQL_COMPILER = 6;
    byte SRC_TABLE_METADATA = 7;
    byte SRC_TABLE_REGISTRY = 5;
    byte SRC_VIEW_WAL_WRITER = 8;
    byte SRC_WAL_WRITER = 4;
    byte SRC_WRITER = 1;

    static boolean isWalOrWriter(byte factoryType) {
        return factoryType == PoolListener.SRC_WRITER || factoryType == PoolListener.SRC_WAL_WRITER;
    }

    void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position);
}
