/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.cairo;

public final class BitmapIndexConstants {
    static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 24;
    static final int KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET = 16;
    static final int KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET = 8;
    static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;
    static final int KEY_ENTRY_SIZE = 32;
    static final int KEY_RESERVED_OFFSET_COUNT_CHECK = 21;
    static final int KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT = 17;
    static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 9;
    static final int KEY_RESERVED_OFFSET_KEY_COUNT = 1;
    static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    static final byte SIGNATURE = (byte) 0xfa;
    static final int VALUE_BLOCK_FILE_RESERVED = 16;
    static final int KEY_FILE_RESERVED = 29;
}
