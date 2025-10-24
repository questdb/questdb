/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import org.jetbrains.annotations.TestOnly;

public class DecimalColumn extends DecimalFunction {
    private final int columnIndex;

    public DecimalColumn(int columnIndex, int columnType) {
        super(columnType);
        this.columnIndex = columnIndex;
    }

    public static DecimalColumn newInstance(int columnIndex, int columnType) {
        return new DecimalColumn(columnIndex, columnType);
    }

    @TestOnly
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        rec.getDecimal128(columnIndex, sink);
    }

    @Override
    public short getDecimal16(Record rec) {
        return rec.getDecimal16(columnIndex);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        rec.getDecimal256(columnIndex, sink);
    }

    @Override
    public int getDecimal32(Record rec) {
        return rec.getDecimal32(columnIndex);
    }

    @Override
    public long getDecimal64(Record rec) {
        return rec.getDecimal64(columnIndex);
    }

    @Override
    public byte getDecimal8(Record rec) {
        return rec.getDecimal8(columnIndex);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.putColumnName(columnIndex);
    }
}
