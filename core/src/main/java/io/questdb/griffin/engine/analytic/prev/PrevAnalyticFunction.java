/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package io.questdb.griffin.engine.analytic.prev;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class PrevAnalyticFunction extends AbstractPrevAnalyticFunction implements Closeable {
    private final long prevPtr;
    private boolean firstPass = true;

    public PrevAnalyticFunction(VirtualColumn valueColumn) {
        super(valueColumn);
        this.prevPtr = Unsafe.malloc(8);
    }

    @Override
    public void prepareFor(Record record) {
        if (firstPass) {
            nextNull = true;
            firstPass = false;
        } else {
            if (nextNull) {
                nextNull = false;
            }
            Unsafe.getUnsafe().putLong(bufPtr, Unsafe.getUnsafe().getLong(prevPtr));
        }

        switch (valueColumn.getType()) {
            case ColumnType.BOOLEAN:
                Unsafe.getUnsafe().putByte(prevPtr, (byte) (valueColumn.getBool(record) ? 1 : 0));
                break;
            case ColumnType.BYTE:
                Unsafe.getUnsafe().putByte(prevPtr, valueColumn.get(record));
                break;
            case ColumnType.DOUBLE:
                Unsafe.getUnsafe().putDouble(prevPtr, valueColumn.getDouble(record));
                break;
            case ColumnType.FLOAT:
                Unsafe.getUnsafe().putFloat(prevPtr, valueColumn.getFloat(record));
                break;
            case ColumnType.SYMBOL:
            case ColumnType.INT:
                Unsafe.getUnsafe().putInt(prevPtr, valueColumn.getInt(record));
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
                Unsafe.getUnsafe().putLong(prevPtr, valueColumn.getLong(record));
                break;
            case ColumnType.SHORT:
                Unsafe.getUnsafe().putShort(prevPtr, valueColumn.getShort(record));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + valueColumn.getType());
        }
    }

    @Override
    public void reset() {
        super.reset();
        firstPass = true;
    }

    @Override
    public void toTop() {
        super.toTop();
        firstPass = true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        Unsafe.free(prevPtr, 8);
    }
}
