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

    /**
     * The key a parquet column is looked up by: the key
     * {@code PageFrameMemoryPool.buildColumnIdMap} files the column's parquet
     * index under, and the writer index a mapping built from a parquet schema
     * (rather than from table metadata) carries for it. The two sides must
     * derive it identically or the lookup misses and the column reads as null,
     * so they derive it here.
     * <p>
     * A parquet field id is the writer index of the QuestDB column that wrote
     * it, so a non-negative id is the key. A negative field id marks a parquet
     * column that belongs to no QuestDB column: the covering index's synthetic
     * {@code key_id} and {@code row_id} carry -1 because the {@code _im} writer
     * requires exactly that to tell them from the covered columns
     * ({@code docs/index-metadata.md}, "Column descriptors"), and a parquet file
     * written outside QuestDB may carry -1 on every column. Those are keyed by
     * position, mapped into the negative half of the space as
     * {@code -(parquetIndex + 1)}, which no writer index can reach.
     * <p>
     * Both sides once substituted the bare parquet position for a negative id,
     * which put such a column in the writer-index space: {@code key_id}, parquet
     * column 0, took id 0 and aliased onto the covered column whose writer index
     * is 0 -- by default the designated timestamp, which
     * {@code cairo.posting.index.auto.include.timestamp} covers. That was not an
     * error, only a wrong answer: {@code key_id} came back as the low 32 bits of
     * each row's timestamp.
     */
    public static int parquetLookupKey(int fieldId, int parquetIndex) {
        return fieldId >= 0 ? fieldId : -(parquetIndex + 1);
    }

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
