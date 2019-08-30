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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.BinarySequence;

public class BinBindVariable extends BinFunction implements StatelessFunction {
    BinarySequence value;

    public BinBindVariable(BinarySequence value) {
        super(0);
        this.value = value;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return value;
    }

    @Override
    public long getBinLen(Record rec) {
        return value == null ? TableUtils.NULL_LEN : value.length();
    }
}
