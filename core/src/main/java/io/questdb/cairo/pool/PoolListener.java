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

package io.questdb.cairo.pool;

import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.TestOnly;

@FunctionalInterface
public interface PoolListener {
    short EV_CREATE = 10;
    short EV_CREATE_EX = 14;
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
    short EV_RETURN = 1;
    short EV_UNEXPECTED_CLOSE = 3;
    short EV_UNLOCKED = 8;
    byte SRC_METADATA = 3;
    byte SRC_READER = 2;
    byte SRC_WAL_WRITER = 4;
    byte SRC_WRITER = 1;

    @TestOnly
    static String eventName(short ev) {
        switch (ev) {
            case EV_RETURN:
                return "EV_RETURN";
            case EV_OUT_OF_POOL_CLOSE:
                return "EV_OUT_OF_POOL_CLOSE";
            case EV_UNEXPECTED_CLOSE:
                return "EV_UNEXPECTED_CLOSE";
            case EV_LOCK_SUCCESS:
                return "EV_LOCK_SUCCESS";
            case EV_LOCK_BUSY:
                return "EV_LOCK_BUSY";
            case EV_UNLOCKED:
                return "EV_UNLOCKED";
            case EV_NOT_LOCKED:
                return "EV_NOT_LOCKED";
            case EV_CREATE:
                return "EV_CREATE";
            case EV_GET:
                return "EV_GET";
            case EV_NOT_LOCK_OWNER:
                return "EV_NOT_LOCK_OWNER";
            case EV_CREATE_EX:
                return "EV_CREATE_EX";
            case EV_EXPIRE:
                return "EV_EXPIRE";
            case EV_LOCK_CLOSE:
                return "EV_LOCK_CLOSE";
            case EV_EX_RESEND:
                return "EV_EX_RESEND";
            case EV_POOL_OPEN:
                return "EV_POOL_OPEN";
            case EV_POOL_CLOSED:
                return "EV_POOL_CLOSED";
            case EV_FULL:
                return "EV_FULL";
            default:
                throw new IllegalArgumentException();
        }
    }

    @TestOnly
    static String factoryName(byte factory) {
        switch (factory) {
            case SRC_METADATA:
                return "SRC_METADATA";
            case SRC_READER:
                return "SRC_READER";
            case SRC_WAL_WRITER:
                return "SRC_WAL_WRITER";
            case SRC_WRITER:
                return "SRC_WRITER";
            default:
                throw new IllegalArgumentException();
        }
    }

    static boolean isWalOrWriter(byte factoryType) {
        return factoryType == PoolListener.SRC_WRITER || factoryType == PoolListener.SRC_WAL_WRITER;
    }

    void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position);
}
