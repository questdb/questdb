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

package com.questdb.cairo.map2;

import com.questdb.std.Unsafe;

public final class DirectMapIterator implements com.questdb.std.ImmutableIterator<DirectMapRecord> {
    private final DirectMapRecord record;
    private int count;
    private long address;

    DirectMapIterator(DirectMapRecord record) {
        this.record = record;
    }

    @Override
    public boolean hasNext() {
        return count > 0;
    }

    @Override
    public DirectMapRecord next() {
        long address = this.address;
        this.address = address + Unsafe.getUnsafe().getInt(address);
        count--;
        return record.of(address);
    }

    DirectMapIterator init(long address, int count) {
        this.address = address;
        this.count = count;
        return this;
    }
}
