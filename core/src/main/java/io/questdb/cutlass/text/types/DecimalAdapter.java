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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.std.Decimal256;
import io.questdb.std.Mutable;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;

public class DecimalAdapter extends AbstractTypeAdapter implements Mutable {
    public static final DecimalAdapter DEFAULT_INSTANCE = new DecimalAdapter(new Decimal256()).of(ColumnType.DECIMAL_DEFAULT_TYPE);

    private final Decimal256 decimal256;
    private int precision;
    private int scale;
    private short tag;
    private int type;

    public DecimalAdapter(Decimal256 decimal256) {
        this.decimal256 = decimal256;
    }

    @Override
    public void clear() {
    }

    @Override
    public int getType() {
        return type;
    }

    public DecimalAdapter of(int type) {
        this.type = type;
        this.precision = ColumnType.getDecimalPrecision(type);
        this.scale = ColumnType.getDecimalScale(type);
        this.tag = ColumnType.tagOf(type);
        return this;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        int len = text.size();

        // We expect the format of decimals to be suffixed with 'm'
        if (len < 2 || text.byteAt(len - 1) != 'm') {
            return false;
        }

        try {
            // We use concurrently decimal256, but it's fine because we don't care about the resulting value, only
            // if parsing succeeds or not.
            decimal256.ofString(text.asAsciiCharSequence(), 0, len - 1, -1, -1, true, false);
            return true;
        } catch (NumericException ignored) {
            return false;
        }
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value, DirectUtf16Sink utf16Sink, DirectUtf8Sink utf8Sink, Decimal256 decimal256) throws Exception {
        decimal256.ofString(value.asAsciiCharSequence(), precision, scale);
        WriterRowUtils.putDecimalQuick(column, decimal256, tag, row);
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        write(row, column, value, null, null, decimal256);
    }
}
