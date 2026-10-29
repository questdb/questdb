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
package io.questdb.cairo;

import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/** Adapts a resolved native frame to the writer's columnar API while its memory is pinned. */
public final class NativeFrameBatchAppender implements QuietCloseable {
    private final IntIntHashMap targetIndexes = new IntIntHashMap();

    public void append(
            PageFrameMemory frame,
            RecordMetadata sourceMetadata,
            SymbolTableSource sourceSymbols,
            IntList destinationWriterIndexes,
            int rowCount,
            TableRecordMetadata target,
            ColumnarRowAppender destination
    ) {
        append(frame, sourceMetadata, sourceSymbols, destinationWriterIndexes, 0, rowCount, target, destination);
    }

    public void append(
            PageFrameMemory frame,
            RecordMetadata sourceMetadata,
            SymbolTableSource sourceSymbols,
            IntList destinationWriterIndexes,
            int rowLo,
            int rowCount,
            TableRecordMetadata target,
            ColumnarRowAppender destination
    ) {
        if (rowLo < 0 || rowCount < 0 || (long) rowLo + rowCount > Integer.MAX_VALUE
                || sourceMetadata.getColumnCount() != frame.getColumnCount()
                || destinationWriterIndexes.size() != frame.getColumnCount()) {
            throw CairoException.nonCritical().put("invalid native frame row/column counts");
        }
        if (rowCount == 0) {
            return;
        }
        targetIndexes.clear();
        for (int i = 0, n = target.getColumnCount(); i < n; i++) {
            if (target.hasColumn(i)) {
                targetIndexes.put(target.getColumnMetadata(i).getOriginalWriterIndex(), i);
            }
        }
        final long metadataVersion = target.getMetadataVersion();
        destination.beginColumnarWrite(rowCount);
        try {
            if (target.getMetadataVersion() != metadataVersion) {
                throw CairoException.schemaMismatch().put("native frame target metadata changed at batch start");
            }
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            boolean isOutOfOrder = false;
            boolean hasTimestamp = target.getTimestampIndex() < 0;
            for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                final int targetIndex = targetIndexes.get(destinationWriterIndexes.getQuick(i));
                final int type = sourceMetadata.getColumnType(i);
                if (targetIndex < 0 || target.getColumnType(targetIndex) != type || frame.getSourceColumnType(i) != -1) {
                    throw CairoException.nonCritical().put("native frame requires resolved matching column types [column=").put(i).put(']');
                }
                final boolean isVariable = ColumnType.isVarSize(type);
                final long data = frame.getPageAddress(i);
                final long aux = isVariable ? frame.getAuxPageAddress(i) : 0;
                if (isVariable ? aux == 0 : data == 0) {
                    // Omitted columns are null-filled by the destination.
                    continue;
                }
                if (isVariable) {
                    final long auxBytes = frame.getAuxPageSizes().get(frame.getColumnOffset() + i);
                    final ColumnTypeDriver driver = ColumnType.getDriver(type);
                    final long offset = driver.getAuxVectorOffset(rowLo);
                    final long required = driver.getAuxVectorSize((long) rowLo + rowCount);
                    if (auxBytes < required) {
                        throw CairoException.nonCritical().put("native frame does not expose complete auxiliary bounds [column=")
                                .put(i).put(", required=").put(required).put(", actual=").put(auxBytes).put(']');
                    }
                    if (type == ColumnType.VARCHAR && frame.getFrameFormat() == PartitionFormat.PARQUET) {
                        destination.putVarcharSliceColumn(targetIndex, aux + offset, rowCount);
                    } else {
                        destination.putNativeColumn(targetIndex, data, frame.getPageSize(i), aux + offset, required - offset, rowCount);
                    }
                } else {
                    final int width = ColumnType.sizeOf(type);
                    final long offset = (long) rowLo * width;
                    final long size = (long) rowCount * width;
                    if (frame.getPageSize(i) < offset + size) {
                        throw CairoException.nonCritical().put("native frame has incomplete data bounds [column=").put(i).put(']');
                    }
                    if (targetIndex == target.getTimestampIndex()) {
                        long previous = Long.MIN_VALUE;
                        for (int row = 0; row < rowCount; row++) {
                            final long timestamp = Unsafe.getLong(data + offset + (long) row * Long.BYTES);
                            minTimestamp = Math.min(minTimestamp, timestamp);
                            maxTimestamp = Math.max(maxTimestamp, timestamp);
                            isOutOfOrder |= timestamp < previous;
                            previous = timestamp;
                        }
                        hasTimestamp = true;
                    }
                    if (ColumnType.isSymbol(type)) {
                        destination.putNativeSymbolColumn(targetIndex, data + offset, rowCount,
                                sourceSymbols == null ? null : sourceSymbols.getSymbolTable(i));
                    } else {
                        destination.putNativeColumn(targetIndex, data + offset, size, 0, 0, rowCount);
                    }
                }
            }
            if (!hasTimestamp) {
                throw CairoException.nonCritical().put("native frame is missing designated timestamp column");
            }
            destination.endColumnarWrite(minTimestamp, maxTimestamp, isOutOfOrder);
        } catch (Throwable th) {
            destination.cancelColumnarWrite();
            throw th;
        }
    }

    @Override
    public void close() {
        targetIndexes.clear();
    }
}
