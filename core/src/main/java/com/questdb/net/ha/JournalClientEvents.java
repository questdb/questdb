/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.net.ha;

public final class JournalClientEvents {
    public static final int EVT_NONE = 0;
    public static final int EVT_RUNNING = 2;
    public static final int EVT_CLIENT_HALT = 4;
    public static final int EVT_CLIENT_EXCEPTION = 8;
    public static final int EVT_INCOMPATIBLE_JOURNAL = 16;
    public static final int EVT_CONNECTED = 32;
    public static final int EVT_AUTH_CONFIG_ERROR = 64;
    public static final int EVT_SERVER_ERROR = 1;
    public static final int EVT_AUTH_ERROR = 128;
    public static final int EVT_TERMINATED = 256;
    public static final int EVT_UNSUB_REJECT = 257;
    public static final int EVT_SERVER_DIED = 258;

    private JournalClientEvents() {
    }
}
