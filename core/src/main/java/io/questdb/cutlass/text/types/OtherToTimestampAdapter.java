/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectByteCharSequence;

public class OtherToTimestampAdapter extends TimestampAdapter implements Mutable {
    private TimestampCompatibleAdapter compatibleAdapter;

    @Override
    public void clear() {
        this.compatibleAdapter = null;
    }

    public long getTimestamp(DirectByteCharSequence value) throws Exception {
        return compatibleAdapter.getTimestamp(value);
    }

    @Override
    public int getType() {
        return ColumnType.TIMESTAMP;
    }

    @Override
    public boolean probe(CharSequence text) {
        return compatibleAdapter.probe(text);
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value) throws Exception {
        row.putTimestamp(column, getTimestamp(value));
    }

    public OtherToTimestampAdapter of(TimestampCompatibleAdapter compatibleAdapter) {
        this.compatibleAdapter = compatibleAdapter;
        return this;
    }
}
