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

package com.questdb.ql;

import com.questdb.std.Unsafe;
import com.questdb.store.AbstractRecordMetadata;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordMetadata;

public class SplitRecordMetadata extends AbstractRecordMetadata {
    private final int columnCount;
    private final RecordColumnMetadata[] columns;
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final int split;

    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;

        int split = a.getColumnCount();
        this.columnCount = split + b.getColumnCount();
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata m = a.getColumnQuick(i);
            columns[i] = m;
        }

        for (int i = 0, c = columnCount - split; i < c; i++) {
            RecordColumnMetadata m = b.getColumnQuick(i);
            columns[i + split] = m;
        }
        this.split = split;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        int index = a.getColumnIndexQuiet(name);
        if (index == -1) {
            index = b.getColumnIndexQuiet(name);
            if (index == -1) {
                return index;
            } else {
                return index + split;
            }
        } else {
            return index;
        }
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public int getTimestampIndex() {
        return a.getTimestampIndex();
    }

    @Override
    public void setAlias(String alias) {
        super.setAlias(alias);
        a.setAlias(alias);
        b.setAlias(alias);
    }
}
