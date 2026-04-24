/*******************************************************************************
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

package io.questdb.cairo.sql;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;

/**
 * Bundles column indexes, writer indexes, and original writer indexes for parquet column mapping.
 * <p>
 * Backed by a single {@link IntList} with interleaved triples:
 * {@code [colIdx0, writerIdx0, origWriterIdx0, colIdx1, writerIdx1, origWriterIdx1, ...]}
 * <p>
 * The original writer index is the root of the replacingIndex chain. For type-converted
 * columns (ALTER COLUMN TYPE), it points to the original column index before any conversions.
 * Parquet files store data under the original writer index as field_id, so a single direct
 * lookup always finds the column regardless of how many type conversions happened.
 */
public class ColumnMapping implements Mutable {
    private final IntList data = new IntList();

    public void addColumn(int columnIndex, int writerIndex, int originalWriterIndex) {
        data.add(columnIndex);
        data.add(writerIndex);
        data.add(originalWriterIndex);
    }

    @Override
    public void clear() {
        data.clear();
    }

    public void copyFrom(ColumnMapping other) {
        data.clear();
        data.addAll(other.data);
    }

    public int getColumnCount() {
        return data.size() / 3;
    }

    public int getColumnIndex(int i) {
        return data.getQuick(3 * i);
    }

    public int getOriginalWriterIndex(int i) {
        return data.getQuick(3 * i + 2);
    }

    public int getWriterIndex(int i) {
        return data.getQuick(3 * i + 1);
    }
}
