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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.str.CharSink;

public class StrColumn extends StrFunction implements StatelessFunction {
    private final int columnIndex;

    public StrColumn(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return rec.getStr(columnIndex);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return rec.getStrB(columnIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        rec.getStr(columnIndex, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        return rec.getStrLen(columnIndex);
    }
}
