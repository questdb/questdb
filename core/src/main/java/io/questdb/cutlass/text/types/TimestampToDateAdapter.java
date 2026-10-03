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
import io.questdb.std.Decimal256;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;

/**
 * Writes text into an existing DATE column with the timestamp format the detector or the
 * user schema chose, converting the parsed timestamp to millis.
 */
public class TimestampToDateAdapter extends AbstractTypeAdapter implements Mutable {
    private TimestampAdapter timestampAdapter;

    @Override
    public void clear() {
        this.timestampAdapter = null;
    }

    @Override
    public int getType() {
        return ColumnType.DATE;
    }

    public TimestampToDateAdapter of(TimestampAdapter timestampAdapter) {
        this.timestampAdapter = timestampAdapter;
        return this;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        return timestampAdapter.probe(text);
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value, DirectUtf16Sink utf16Sink, DirectUtf8Sink utf8Sink, Decimal256 decimal256) throws Exception {
        row.putDate(column, ColumnType.getTimestampDriver(timestampAdapter.getType()).toDate(timestampAdapter.getTimestamp(value, utf16Sink)));
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        row.putDate(column, ColumnType.getTimestampDriver(timestampAdapter.getType()).toDate(timestampAdapter.getTimestamp(value)));
    }
}
