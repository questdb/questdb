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

package com.questdb.cairo.map;

import com.questdb.std.ImmutableIterator;

public class CompactMapCursor implements ImmutableIterator<MapRecord> {

    private final CompactMapRecord record;
    private long offsetHi;
    private long nextOffset;

    public CompactMapCursor(CompactMapRecord record) {
        this.record = record;
    }

    @Override
    public boolean hasNext() {
        if (nextOffset < offsetHi) {
            record.of(nextOffset);
            return true;
        }
        return false;
    }

    @Override
    public MapRecord next() {
        nextOffset = record.getNextRecordOffset();
        return record;
    }

    void of(long offsetHi) {
        this.nextOffset = 0;
        this.offsetHi = offsetHi;
    }
}
