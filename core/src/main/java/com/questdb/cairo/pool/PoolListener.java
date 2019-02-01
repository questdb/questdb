/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.pool;

public interface PoolListener {
    byte SRC_WRITER = 1;
    byte SRC_READER = 2;

    short EV_RETURN = 1;
    short EV_OUT_OF_POOL_CLOSE = 2;
    short EV_UNEXPECTED_CLOSE = 3;
    //    short EV_COMMIT_EX = 4;
//    short EV_NOT_IN_POOL = 5;
    short EV_LOCK_SUCCESS = 6;
    short EV_LOCK_BUSY = 7;
    short EV_UNLOCKED = 8;
    short EV_NOT_LOCKED = 9;
    short EV_CREATE = 10;
    short EV_GET = 11;
    short EV_NOT_LOCK_OWNER = 12;
    //    short EV_INCOMPATIBLE = 13;
    short EV_CREATE_EX = 14;
    //    short EV_CLOSE_EX = 15;
    short EV_EXPIRE = 17;
    //    short EV_EXPIRE_EX = 18;
    short EV_LOCK_CLOSE = 19;
    //    short EV_LOCK_CLOSE_EX = 20;
    short EV_EX_RESEND = 21;
    short EV_POOL_OPEN = 23;
    short EV_POOL_CLOSED = 24;
    short EV_FULL = 25;

    void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position);
}
