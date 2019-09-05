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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

class UnionRecord implements Record {
    private Record base;
    private IntList columnMapIndex;

    public void of(Record base, IntList columnMapIndex) {
        this.base = base;
        this.columnMapIndex = columnMapIndex;
    }


    @Override
    public BinarySequence getBin(int col) {
        final int baseIndex = columnMapIndex.getQuick(col);
        if (baseIndex == -1) {
            return null;
        }
        return base.getBin(baseIndex);
    }

    @Override
    public int getInt(int col) {
        final int baseIndex = columnMapIndex.getQuick(col);
        if (baseIndex == -1) {
            return Numbers.INT_NaN;
        }
        return base.getInt(baseIndex);
    }

    @Override
    public long getLong(int col) {
        final int baseIndex = columnMapIndex.getQuick(col);
        if (baseIndex == -1) {
            return Numbers.LONG_NaN;
        }
        return base.getLong(baseIndex);

    }
}
