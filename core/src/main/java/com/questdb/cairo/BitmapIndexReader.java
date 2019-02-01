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

package com.questdb.cairo;


import com.questdb.cairo.sql.RowCursor;

import java.io.Closeable;

public interface BitmapIndexReader extends Closeable {

    int DIR_FORWARD = 1;
    int DIR_BACKWARD = 2;

    @Override
    default void close() {
    }

    /**
     * Setup value cursor. Values in this cursor will be bounded by provided
     * minimum and maximum, both of which are inclusive. Order of values is
     * determined by specific implementations of this method.
     *
     * @param cachedInstance when this parameters is true, index reader may return singleton instance of cursor.
     * @param key            index key
     * @param minValue       inclusive minimum value
     * @param maxValue       inclusive maximum value
     * @return index value cursor
     */
    RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue);

    int getKeyCount();

    boolean isOpen();
}
