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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntIntHashMap;

/**
 * Resolves the columns of an exported Parquet page frame to the columns of the frame's Parquet
 * file. The file keeps the columns its table had when the file was written, in their order then,
 * and tags each with the column's writer index as its column id. The exported query reads its
 * own columns in its own order, so the file's i-th column is the query's i-th column only by
 * chance: a projection, a reordering or a DROP COLUMN after the file was written breaks it.
 * {@link io.questdb.cairo.sql.PageFrameMemoryPool} resolves query columns the same way.
 */
final class ParquetExportColumnResolver {
    /**
     * The file has no column for the query column, for example because the column was added
     * after the file was written. The column reads as NULL across the frame.
     */
    static final int ABSENT = -1;
    private final IntIntHashMap columnIdToParquetIndex = new IntIntHashMap();

    /**
     * Appends to {@code parquetColumns} one entry per query column: the index of the file column
     * that holds its data, or {@link #ABSENT}.
     *
     * @param columnMapping  the writer indexes of the query columns, from the frame's cursor
     * @param metadata       the query columns, in the order the export writes them
     * @param decoder        the decoder of the frame's Parquet file
     * @param parquetColumns receives the file column of each query column
     */
    void resolve(ColumnMapping columnMapping, RecordMetadata metadata, ParquetDecoder decoder, DirectIntList parquetColumns) {
        final int columnCount = metadata.getColumnCount();
        assert columnMapping.getColumnCount() == columnCount
                : "column mapping must cover every query column [mapping=" + columnMapping.getColumnCount() + ", columns=" + columnCount + ']';
        columnIdToParquetIndex.clear();
        for (int parquetIndex = 0, n = decoder.getColumnCount(); parquetIndex < n; parquetIndex++) {
            final int columnId = decoder.getColumnId(parquetIndex);
            // A file that QuestDB did not write has no column ids; read_parquet() then
            // identifies its columns by position, see ReadParquetRecordCursor.canProjectMetadata().
            columnIdToParquetIndex.put(columnId < 0 ? parquetIndex : columnId, parquetIndex);
        }
        for (int i = 0; i < columnCount; i++) {
            parquetColumns.add(findParquetColumn(columnMapping, metadata, decoder, i));
        }
    }

    private static CairoException conversionError(RecordMetadata metadata, int columnIndex, int fileColumnType) {
        return CairoException.critical(0)
                .put("parquet export cannot convert column type [column=").put(metadata.getColumnName(columnIndex))
                .put(", fileType=").put(ColumnType.nameOf(fileColumnType))
                .put(", columnType=").put(ColumnType.nameOf(metadata.getColumnType(columnIndex)))
                .put(']');
    }

    // The export decodes a file column straight into the encoder's layout of the query column,
    // with no conversion, so the file column must hold the query column's type.
    private int findParquetColumn(ColumnMapping columnMapping, RecordMetadata metadata, ParquetDecoder decoder, int columnIndex) {
        final int writerIndex = columnMapping.getWriterIndex(columnIndex);
        if (writerIndex < 0) {
            // A column with no writer index, such as an aggregate that a WINDOW JOIN with a false
            // ON clause pads with NULL, has no data.
            return ABSENT;
        }
        final int parquetIndex = columnIdToParquetIndex.get(writerIndex);
        if (parquetIndex == ABSENT) {
            // ALTER COLUMN TYPE gives the column a new writer index, while the file keeps the
            // column's data, in its old type, under the original one. The column needs the
            // conversion PageFrameMemoryPool applies to queries, so
            // ParquetExportMode.determineExportMode() routes it through a temp table instead.
            final int originalWriterIndex = columnMapping.getOriginalWriterIndex(columnIndex);
            final int originalParquetIndex = originalWriterIndex != writerIndex
                    ? columnIdToParquetIndex.get(originalWriterIndex)
                    : ABSENT;
            if (originalParquetIndex != ABSENT) {
                throw conversionError(metadata, columnIndex, decoder.getColumnType(originalParquetIndex));
            }
            return ABSENT;
        }
        // The file wrote this very column, so its type matches; read_parquet() reads a SYMBOL
        // column of a file that QuestDB wrote as VARCHAR. Check the tag all the same: the
        // encoder must never receive a buffer laid out for another type.
        final int fileColumnType = decoder.getColumnType(parquetIndex);
        final int fileTag = ColumnType.tagOf(fileColumnType);
        final int tag = ColumnType.tagOf(metadata.getColumnType(columnIndex));
        if (fileTag != tag && (fileTag != ColumnType.SYMBOL || tag != ColumnType.VARCHAR)) {
            throw conversionError(metadata, columnIndex, fileColumnType);
        }
        return parquetIndex;
    }
}
