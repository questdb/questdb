/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.Numbers;
import io.questdb.std.str.DirectUtf8Sequence;

/**
 * Writes text into an existing TIMESTAMP column when the detected type is neither a timestamp
 * format nor timestamp-compatible. Parses the text like INSERT's implicit VARCHAR to TIMESTAMP
 * cast for the column's precision and throws ImplicitCastException on text the cast rejects,
 * so the importer counts it as a column error.
 */
public final class TimestampCastAdapter extends TimestampAdapter {

    public static final TimestampCastAdapter MICRO_INSTANCE = new TimestampCastAdapter(ColumnType.TIMESTAMP_MICRO);
    public static final TimestampCastAdapter NANO_INSTANCE = new TimestampCastAdapter(ColumnType.TIMESTAMP_NANO);
    private final TimestampDriver driver;
    private final int timestampType;

    private TimestampCastAdapter(int timestampType) {
        this.timestampType = timestampType;
        this.driver = ColumnType.getTimestampDriver(timestampType);
    }

    public static TimestampCastAdapter forTimestampType(int timestampType) {
        return timestampType == ColumnType.TIMESTAMP_NANO ? NANO_INSTANCE : MICRO_INSTANCE;
    }

    @Override
    public long getTimestamp(DirectUtf8Sequence value) {
        // the implicit cast rejects the 'null' keyword, which the numeric adapters store as NULL
        return SqlKeywords.isNullKeyword(value) ? Numbers.LONG_NULL : driver.implicitCastVarchar(value);
    }

    @Override
    public int getType() {
        return timestampType;
    }

    @Override
    public boolean isDesignatedTimestampSupported() {
        return false;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) {
        row.putTimestamp(column, getTimestamp(value));
    }
}
