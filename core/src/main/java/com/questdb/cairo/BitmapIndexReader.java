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

package com.questdb.cairo;

import com.questdb.common.RowCursor;

import java.io.Closeable;

public interface BitmapIndexReader extends Closeable {
    @Override
    default void close() {
    }

    /**
     * Creates cursor for index values for the given key. Cursor should be treated as mutable
     * instance. Typical BitmapIndexReader implementation will return same object instance
     * configured for the required parameters.
     * <p>
     * Returned values are capped to given maximum inclusive.
     *
     * @param key      index key
     * @param maxValue inclusive maximum value
     * @return index value cursor
     */
    RowCursor getCursor(int key, long maxValue);

    int getKeyCount();

    boolean isOpen();
}
