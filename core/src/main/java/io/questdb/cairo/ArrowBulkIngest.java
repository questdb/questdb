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

package io.questdb.cairo;

import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8String;

/**
 * Bulk writer for TableWriter from pre-extracted, column-major primitive
 * arrays. Called by the errand Python layer to avoid per-row / per-cell JPype
 * overhead when ingesting pyarrow Tables through
 * {@code LocalEngine.from_arrow}.
 *
 * <p>Each column is passed as a typed primitive array (or an Object[] for
 * reference types). The row loop and per-cell type dispatch run inside the
 * JVM, collapsing what would otherwise be millions of JNI/JPype calls into
 * a single {@link #copy} invocation.
 *
 * <p>Nulls are represented as either:
 * <ul>
 *   <li>a QuestDB sentinel value already baked into the primitive array
 *       (e.g. {@link Numbers#LONG_NULL} for LONG/DATE/TIMESTAMP, or
 *       {@link Numbers#INT_NULL} for INT/IPv4), in which case the caller
 *       passes {@code null} for the column's entry in {@code nullMasks};</li>
 *   <li>a boolean[] of per-row validity flags ({@code true} means null) for
 *       column types without a numeric sentinel (BOOLEAN, BYTE, SHORT, CHAR,
 *       FLOAT, DOUBLE, and all reference types). The column value at a null
 *       row is skipped so TableWriter fills its own default null.</li>
 * </ul>
 */
public final class ArrowBulkIngest {

    private ArrowBulkIngest() {
    }

    /**
     * Append {@code nRows} rows to {@code writer} using the supplied
     * column-major data.
     *
     * @param writer         the open TableWriter
     * @param tsColIndex     index of the designated timestamp column in the
     *                       table metadata, or -1 if the table is
     *                       non-partitioned / has no timestamp
     * @param tsMicros       timestamps (micros since epoch) for each row, or
     *                       {@code null} if no timestamp column. Nulls are
     *                       carried as {@link Numbers#LONG_NULL}.
     * @param colIndices     table-metadata column index for each entry in
     *                       {@code colData} (parallel arrays)
     * @param colTypeTags    {@link ColumnType} tag for each column (parallel
     *                       to {@code colIndices})
     * @param colData        per-column typed primitive array (long[], int[],
     *                       short[], byte[], double[], float[], boolean[])
     *                       or Object[] for reference types (STRING / VARCHAR
     *                       / SYMBOL / UUID carry {@code CharSequence}
     *                       elements; CHAR carries boxed {@code Integer});
     *                       {@code null} skips the column entirely
     * @param nullMasks      per-column {@code boolean[]} validity mask (true =
     *                       null) or {@code null} when the column uses
     *                       in-band sentinels / has no nulls
     * @param nRows          row count
     * @param commitInterval rows between intermediate {@code writer.commit()}
     *                       calls (0 = never commit mid-batch)
     */
    public static void copy(
            TableWriter writer,
            int tsColIndex,
            long[] tsMicros,
            int[] colIndices,
            int[] colTypeTags,
            Object[] colData,
            boolean[][] nullMasks,
            int nRows,
            int commitInterval
    ) {
        final int nCols = colIndices.length;
        int rowsSinceCommit = 0;

        for (int r = 0; r < nRows; r++) {
            final TableWriter.Row row;
            if (tsMicros != null) {
                row = writer.newRow(tsMicros[r]);
            } else if (tsColIndex >= 0) {
                row = writer.newRow(Numbers.LONG_NULL);
            } else {
                row = writer.newRow();
            }

            for (int c = 0; c < nCols; c++) {
                final Object data = colData[c];
                if (data == null) {
                    continue;
                }
                final boolean[] mask = nullMasks == null ? null : nullMasks[c];
                if (mask != null && mask[r]) {
                    continue;
                }
                final int colIdx = colIndices[c];
                final int tag = colTypeTags[c];
                switch (tag) {
                    case ColumnType.BOOLEAN:
                        row.putBool(colIdx, ((boolean[]) data)[r]);
                        break;
                    case ColumnType.BYTE:
                        row.putByte(colIdx, ((byte[]) data)[r]);
                        break;
                    case ColumnType.SHORT:
                        row.putShort(colIdx, ((short[]) data)[r]);
                        break;
                    case ColumnType.CHAR:
                        row.putChar(colIdx, (char) ((int[]) data)[r]);
                        break;
                    case ColumnType.INT:
                        row.putInt(colIdx, ((int[]) data)[r]);
                        break;
                    case ColumnType.IPv4:
                        row.putIPv4(colIdx, ((int[]) data)[r]);
                        break;
                    case ColumnType.LONG:
                        row.putLong(colIdx, ((long[]) data)[r]);
                        break;
                    case ColumnType.DATE:
                        row.putDate(colIdx, ((long[]) data)[r]);
                        break;
                    case ColumnType.TIMESTAMP:
                        row.putTimestamp(colIdx, ((long[]) data)[r]);
                        break;
                    case ColumnType.FLOAT:
                        row.putFloat(colIdx, ((float[]) data)[r]);
                        break;
                    case ColumnType.DOUBLE:
                        row.putDouble(colIdx, ((double[]) data)[r]);
                        break;
                    case ColumnType.STRING: {
                        final Object v = ((Object[]) data)[r];
                        if (v != null) {
                            row.putStr(colIdx, (CharSequence) v);
                        }
                        break;
                    }
                    case ColumnType.SYMBOL: {
                        final Object v = ((Object[]) data)[r];
                        if (v != null) {
                            row.putSym(colIdx, (CharSequence) v);
                        }
                        break;
                    }
                    case ColumnType.UUID: {
                        final Object v = ((Object[]) data)[r];
                        if (v != null) {
                            row.putUuid(colIdx, (CharSequence) v);
                        }
                        break;
                    }
                    case ColumnType.VARCHAR: {
                        final Object v = ((Object[]) data)[r];
                        if (v != null) {
                            row.putVarchar(colIdx, (Utf8String) v);
                        }
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException(
                                "ArrowBulkIngest: unsupported column type tag " + tag
                                        + " at column index " + colIdx
                        );
                }
            }

            row.append();
            rowsSinceCommit++;
            if (commitInterval > 0 && rowsSinceCommit >= commitInterval) {
                writer.commit();
                rowsSinceCommit = 0;
            }
        }

        if (rowsSinceCommit > 0) {
            writer.commit();
        }
    }
}
