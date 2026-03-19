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

import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

/**
 * Bundles column indexes and writer indexes (field_ids) for parquet column mapping.
 * <p>
 * Backed by a single {@link IntList} with interleaved pairs:
 * {@code [colIdx0, writerIdx0, colIdx1, writerIdx1, ...]}
 * <p>
 * Optionally holds a reference to a writer-index-to-dense-index map from
 * {@link io.questdb.cairo.TableReaderMetadata}. This map covers all historical
 * writer indexes (including those from type-converted columns) and enables
 * parquet column resolution after ALTER COLUMN TYPE.
 */
public class ColumnMapping implements Mutable {
    private final IntList data = new IntList();
    // Nullable. Set only for table reader cursors to enable type-converted column lookup.
    private IntIntHashMap writerIndexToDenseIndex;

    public void addColumn(int columnIndex, int writerIndex) {
        data.add(columnIndex);
        data.add(writerIndex);
    }

    @Override
    public void clear() {
        data.clear();
        writerIndexToDenseIndex = null;
    }

    public void copyFrom(ColumnMapping other) {
        data.clear();
        data.addAll(other.data);
        writerIndexToDenseIndex = other.writerIndexToDenseIndex;
    }

    public int getColumnCount() {
        return data.size() / 2;
    }

    public int getColumnIndex(int i) {
        return data.getQuick(2 * i);
    }

    public int getDenseIndexForWriterIndex(int writerIndex) {
        if (writerIndexToDenseIndex != null) {
            return writerIndexToDenseIndex.get(writerIndex);
        }
        return -1;
    }

    public int getWriterIndex(int i) {
        return data.getQuick(2 * i + 1);
    }

    public boolean hasWriterIndexToDenseIndex() {
        return writerIndexToDenseIndex != null;
    }

    public void setWriterIndexToDenseIndex(IntIntHashMap map) {
        this.writerIndexToDenseIndex = map;
    }
}
