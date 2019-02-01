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

public final class PoolConstants {
    public static final int CR_POOL_CLOSE = 1;
    public static final int CR_NAME_LOCK = 2;
    public static final int CR_IDLE = 3;
    public static final int CR_REOPEN = 4;
    public static final long UNALLOCATED = -1L;

    public static String closeReasonText(int reason) {
        switch (reason) {
            case CR_POOL_CLOSE:
                return "POOL_CLOSED";
            case CR_NAME_LOCK:
                return "LOCKED";
            case CR_IDLE:
                return "IDLE";
            case CR_REOPEN:
                return "REOPEN";
            default:
                return "UNKNOWN";
        }
    }
}
