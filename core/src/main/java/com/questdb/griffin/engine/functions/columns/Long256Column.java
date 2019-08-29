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

package com.questdb.griffin.engine.functions.columns;

import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.Long256Function;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.std.Long256;
import com.questdb.std.str.CharSink;

public class Long256Column extends Long256Function implements StatelessFunction {
    private final int columnIndex;

    public Long256Column(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        rec.getLong256(columnIndex, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return rec.getLong256A(columnIndex);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return rec.getLong256B(columnIndex);
    }
}
