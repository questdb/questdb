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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Vect;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

/**
 * Scans table partitions forward using bitmap index to find the earliest row
 * for each distinct symbol value. Supports geohash prefix filtering.
 */
class EarliestByAllIndexedRecordCursor extends AbstractAscendingRecordListCursor {
    private final int columnIndex;
    private final IntHashSet foundKeys;
    private final DirectLongList prefixes;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int keyCount;

    public EarliestByAllIndexedRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull DirectLongList prefixes
    ) {
        super(configuration, metadata, rows);
        this.columnIndex = columnIndex;
        this.prefixes = prefixes;
        this.foundKeys = new IntHashSet();
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            isOpen = true;
        }
        super.of(pageFrameCursor, executionContext);
        circuitBreaker = executionContext.getCircuitBreaker();
        keyCount = -1;
        foundKeys.clear();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index forward scan").meta("on").putColumnName(columnIndex);

        if (prefixes.size() > 2) {
            int geoHashColumnIndex = (int) prefixes.get(0);
            int geoHashColumnType = (int) prefixes.get(1);
            int geoHashBits = ColumnType.getGeoHashBits(geoHashColumnType);

            if (geoHashColumnIndex > -1 && ColumnType.isGeoHash(geoHashColumnType)) {
                sink.attr("filter").putColumnName(geoHashColumnIndex).val(" within(");
                for (long i = 2, n = prefixes.size(); i < n; i += 2) {
                    if (i > 2) {
                        sink.val(',');
                    }
                    sink.val(prefixes.get(i), geoHashBits);
                }
                sink.val(')');
            }
        }
    }

    @Override
    protected void buildTreeMap() {
        if (keyCount < 0) {
            keyCount = getSymbolTable(columnIndex).getSymbolCount() + 1; // +1 for null
            rows.setCapacity(keyCount);
        }

        boolean hasGeoHashFilter = prefixes.size() > 2;
        int geoHashColumnIndex = hasGeoHashFilter ? (int) prefixes.get(0) : -1;
        int geoHashColumnType = hasGeoHashFilter ? (int) prefixes.get(1) : ColumnType.UNDEFINED;

        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_FORWARD);

            // iterate all symbol keys (0..keyCount-1, where key 0 = null symbol)
            for (int key = 0; key < keyCount; key++) {
                if (foundKeys.contains(key)) {
                    continue; // already found earliest for this key
                }

                RowCursor rowCursor = indexReader.getCursor(true, key, partitionLo, partitionHi);
                while (rowCursor.hasNext()) {
                    long row = rowCursor.next();
                    recordA.setRowIndex(row);

                    if (hasGeoHashFilter && !matchesGeoHashPrefix(geoHashColumnIndex, geoHashColumnType)) {
                        continue;
                    }

                    // first matching row for this key in forward order = earliest
                    foundKeys.add(key);
                    rows.add(Rows.toRowID(frameIndex, row));
                    break;
                }
            }

            if (foundKeys.size() >= keyCount) {
                break; // found earliest for all symbol values
            }
        }
        // sort rows by row ID to ensure ascending timestamp order
        Vect.sortULongAscInPlace(rows.getAddress(), rows.size());
    }

    private boolean matchesGeoHashPrefix(int geoHashColumnIndex, int geoHashColumnType) {
        long cellValue = switch (ColumnType.tagOf(geoHashColumnType)) {
            case ColumnType.GEOBYTE -> recordA.getGeoByte(geoHashColumnIndex);
            case ColumnType.GEOSHORT -> recordA.getGeoShort(geoHashColumnIndex);
            case ColumnType.GEOINT -> recordA.getGeoInt(geoHashColumnIndex);
            case ColumnType.GEOLONG -> recordA.getGeoLong(geoHashColumnIndex);
            default -> {
                yield -1;
            }
        };

        if (cellValue == -1) {
            return false;
        }

        // prefixes are stored as pairs: (normalizedHash, mask)
        // matching: (cellValue & mask) == normalizedHash
        for (long i = 2, n = prefixes.size(); i < n; i += 2) {
            long normHash = prefixes.get(i);
            long mask = prefixes.get(i + 1);
            if ((cellValue & mask) == normHash) {
                return true;
            }
        }
        return false;
    }
}
