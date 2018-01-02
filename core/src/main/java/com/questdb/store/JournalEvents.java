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

package com.questdb.store;

public final class JournalEvents {
    public static final int EVT_JNL_ALREADY_SUBSCRIBED = 1;
    public static final int EVT_JNL_INCOMPATIBLE = 2;
    public static final int EVT_JNL_TRANSACTION_REFUSED = 3;
    public static final int EVT_JNL_UNKNOWN_TRANSACTION = 4;
    public static final int EVT_JNL_SERVER_ERROR = 5;
    public static final int EVT_JNL_SUBSCRIBED = 6;

    private JournalEvents() {
    }
}
