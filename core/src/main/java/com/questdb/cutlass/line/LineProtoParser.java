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

package com.questdb.cutlass.line;

public interface LineProtoParser {
    int EVT_MEASUREMENT = 1;
    int EVT_TAG_VALUE = 2;
    int EVT_FIELD_VALUE = 3;
    int EVT_TAG_NAME = 4;
    int EVT_FIELD_NAME = 5;
    int EVT_TIMESTAMP = 6;

    int ERROR_EXPECTED = 1;
    int ERROR_ENCODING = 2;
    int ERROR_EMPTY = 3;

    void onError(int position, int state, int code);

    void onEvent(CachedCharSequence token, int type, CharSequenceCache cache);

    void onLineEnd(CharSequenceCache cache);
}
