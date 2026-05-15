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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.BoolList;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Iterates parquet files matched by a glob, exposing their rows as a single cursor.
 * Wraps a {@link ReadParquetRecordCursor} that is repointed at each file in turn.
 * <p>
 * Hive-style {@code key=value} segments in the directory path between
 * {@code nonGlobRoot} and the file name become virtual columns appended after the
 * parquet schema columns. Column types are inferred from observed values at
 * factory creation; this cursor only parses values into the typed slot decided
 * upstream.
 */
public class HivePartitionedReadParquetRecordCursor implements NoRandomAccessRecordCursor {
    // The factory enumerates files once at planning time and owns matchedFiles;
    // this cursor borrows a reference and walks it with globIndex. Avoids the
    // re-enumeration the previous globCursor-driven design did on every toTop /
    // skipRows / calculateSize.
    private final DirectUtf8StringList matchedFiles;
    private final int nonGlobRootLen;
    private final int parquetColumnCount;
    private final ReadParquetRecordCursor parquetCursor;
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private final LongList partitionLongValues = new LongList();
    private final BoolList partitionValuePresent = new BoolList();
    private final ObjList<Utf8StringSink> partitionVarcharValues;
    private final Path path = new Path(MemoryTag.NATIVE_PATH);
    private final HivePartitionedRecord wrappingRecord;
    private SqlExecutionContext executionContext;
    private int globIndex = 0;
    private boolean isOpen = true;
    private boolean parquetCursorPositioned;

    public HivePartitionedReadParquetRecordCursor(
            DirectUtf8StringList matchedFiles,
            ReadParquetRecordCursor parquetCursor,
            int nonGlobRootByteLen,
            int parquetColumnCount,
            ObjList<String> partitionColumnNames,
            IntList partitionColumnTypes
    ) {
        this.matchedFiles = matchedFiles;
        this.parquetCursor = parquetCursor;
        this.nonGlobRootLen = nonGlobRootByteLen;
        this.parquetColumnCount = parquetColumnCount;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        final int partitionCount = partitionColumnNames.size();
        this.partitionVarcharValues = new ObjList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionLongValues.add(0L);
            partitionValuePresent.add(false);
            // Only varchar columns need a backing sink, but allocating one per column
            // keeps lookup branchless and partition column counts are tiny.
            partitionVarcharValues.add(new Utf8StringSink());
        }
        this.wrappingRecord = new HivePartitionedRecord();
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        // Fast path: walk each matched file, pull row count from its metadata, move on.
        globIndex = 0;
        parquetCursorPositioned = false;
        while (!circuitBreaker.checkIfTripped() && globIndex < matchedFiles.size()) {
            switchToNextParquetFileMetadata(matchedFiles.getQuick(globIndex++));
            parquetCursor.calculateSize(circuitBreaker, counter);
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(parquetCursor);
            // matchedFiles is owned by the factory; do not free it here.
            Misc.free(path);
            for (int i = 0, n = partitionVarcharValues.size(); i < n; i++) {
                partitionVarcharValues.getQuick(i).clear();
            }
        }
    }

    @Override
    public Record getRecord() {
        return wrappingRecord;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (parquetCursorPositioned && parquetCursor.hasNext()) {
                return true;
            }
            if (globIndex >= matchedFiles.size()) {
                return false;
            }
            switchToNextParquetFile(matchedFiles.getQuick(globIndex++));
        }
    }

    public void of(SqlExecutionContext executionContext) {
        this.executionContext = executionContext;
        parquetCursorPositioned = false;
        globIndex = 0;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void skipRows(Counter counter) {
        globIndex = 0;
        parquetCursorPositioned = false;
        while (counter.get() > 0 && globIndex < matchedFiles.size()) {
            switchToNextParquetFileMetadata(matchedFiles.getQuick(globIndex++));
            long remainingBefore = counter.get();
            long rowsInFile = parquetCursor.size();
            if (rowsInFile <= remainingBefore) {
                counter.dec(rowsInFile);
            } else {
                switchCurrentFileToFullRead();
                parquetCursor.skipRows(counter);
                return;
            }
        }
    }

    @Override
    public void toTop() {
        parquetCursorPositioned = false;
        globIndex = 0;
    }

    private void parsePartitionValues(Utf8Sequence filePath) {
        final int partitionCount = partitionColumnNames.size();
        for (int i = 0; i < partitionCount; i++) {
            partitionValuePresent.setQuick(i, false);
            partitionLongValues.setQuick(i, 0L);
            partitionVarcharValues.getQuick(i).clear();
        }
        if (partitionCount == 0) {
            return;
        }
        // Walk DIRECTORY segments only - the filename is intentionally excluded so a
        // name like 'foo=bar.parquet' isn't misread as a partition. Mirrors the
        // boundary applied by ReadParquetFunctionFactory.parsePartitionColumns at
        // planning time.
        final int dirEnd = lastPathSeparator(filePath, nonGlobRootLen);
        if (dirEnd < 0) {
            return;
        }
        int segStart = Math.min(nonGlobRootLen, dirEnd);
        while (segStart < dirEnd) {
            int segEnd = segStart;
            while (segEnd < dirEnd) {
                byte b = filePath.byteAt(segEnd);
                if (b == '/' || b == Files.SEPARATOR) {
                    break;
                }
                segEnd++;
            }
            int eqIdx = -1;
            for (int i = segStart; i < segEnd; i++) {
                if (filePath.byteAt(i) == '=') {
                    eqIdx = i;
                    break;
                }
            }
            if (eqIdx > segStart && eqIdx < segEnd - 1) {
                final int keyLen = eqIdx - segStart;
                int matchedIdx = -1;
                for (int i = 0; i < partitionCount; i++) {
                    String name = partitionColumnNames.getQuick(i);
                    if (name.length() == keyLen) {
                        boolean ok = true;
                        for (int j = 0; j < keyLen; j++) {
                            if (filePath.byteAt(segStart + j) != (byte) name.charAt(j)) {
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            matchedIdx = i;
                            break;
                        }
                    }
                }
                if (matchedIdx >= 0) {
                    storePartitionValue(matchedIdx, filePath, eqIdx + 1, segEnd);
                }
            }
            if (segEnd >= dirEnd) {
                break;
            }
            segStart = segEnd + 1;
        }
    }

    private static int lastPathSeparator(Utf8Sequence path, int from) {
        for (int i = path.size() - 1; i >= from; i--) {
            byte b = path.byteAt(i);
            if (b == '/' || b == Files.SEPARATOR) {
                return i;
            }
        }
        return -1;
    }

    private void storePartitionValue(int idx, Utf8Sequence filePath, int lo, int hi) {
        final int type = partitionColumnTypes.getQuick(idx);
        if (type == ColumnType.VARCHAR) {
            partitionVarcharValues.getQuick(idx).put(filePath, lo, hi);
            partitionValuePresent.setQuick(idx, true);
            return;
        }
        // Numeric/temporal types: decode to UTF-16 then parse. If parsing fails for any reason
        // (unexpected new file written after planning), record the column as NULL for this file
        // rather than aborting the entire query.
        final StringSink sink = Misc.getThreadLocalSink();
        Utf8s.utf8ToUtf16(filePath, lo, hi, sink);
        try {
            long value;
            switch (type) {
                case ColumnType.INT:
                    value = Numbers.parseInt(sink);
                    break;
                case ColumnType.LONG:
                    value = Numbers.parseLong(sink);
                    break;
                case ColumnType.DATE:
                    value = DateFormatUtils.parseDate(sink);
                    break;
                case ColumnType.TIMESTAMP:
                    value = MicrosFormatUtils.parseTimestamp(sink);
                    break;
                case ColumnType.DOUBLE:
                    value = Double.doubleToLongBits(Numbers.parseDouble(sink));
                    break;
                default:
                    return;
            }
            partitionLongValues.setQuick(idx, value);
            partitionValuePresent.setQuick(idx, true);
        } catch (NumericException ignored) {
            // Leave the value marked as absent.
        }
    }

    private void switchCurrentFileToFullRead() {
        try {
            parquetCursor.of(path.$(), executionContext);
            parquetCursorPositioned = true;
        } catch (io.questdb.cairo.sql.TableReferenceOutOfDateException e) {
            throw CairoException.nonCritical()
                    .put("parquet schema mismatch: file '")
                    .put(path)
                    .put("' is incompatible with the schema of the first matched file");
        } catch (SqlException e) {
            throw CairoException.nonCritical()
                    .put("failed to read parquet file [path=")
                    .put(path).put(", msg=").put(e.getFlyweightMessage()).put(']');
        }
    }

    private void switchToNextParquetFile(Utf8Sequence filePath) {
        parsePartitionValues(filePath);
        path.of(filePath);
        switchCurrentFileToFullRead();
    }

    private void switchToNextParquetFileMetadata(Utf8Sequence filePath) {
        path.of(filePath);
        parquetCursor.ofMetadata(path.$());
        parquetCursorPositioned = false;
    }

    private class HivePartitionedRecord implements Record {
        @Override
        public ArrayView getArray(int col, int columnType) {
            return parquetCursor.getRecord().getArray(col, columnType);
        }

        @Override
        public BinarySequence getBin(int col) {
            return parquetCursor.getRecord().getBin(col);
        }

        @Override
        public long getBinLen(int col) {
            return parquetCursor.getRecord().getBinLen(col);
        }

        @Override
        public boolean getBool(int col) {
            return parquetCursor.getRecord().getBool(col);
        }

        @Override
        public byte getByte(int col) {
            return parquetCursor.getRecord().getByte(col);
        }

        @Override
        public char getChar(int col) {
            return parquetCursor.getRecord().getChar(col);
        }

        @Override
        public long getDate(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getDate(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionLongValues.getQuick(idx) : Numbers.LONG_NULL;
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            parquetCursor.getRecord().getDecimal128(col, sink);
        }

        @Override
        public short getDecimal16(int col) {
            return parquetCursor.getRecord().getDecimal16(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            parquetCursor.getRecord().getDecimal256(col, sink);
        }

        @Override
        public int getDecimal32(int col) {
            return parquetCursor.getRecord().getDecimal32(col);
        }

        @Override
        public long getDecimal64(int col) {
            return parquetCursor.getRecord().getDecimal64(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return parquetCursor.getRecord().getDecimal8(col);
        }

        @Override
        public double getDouble(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getDouble(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? Double.longBitsToDouble(partitionLongValues.getQuick(idx)) : Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            return parquetCursor.getRecord().getFloat(col);
        }

        @Override
        public byte getGeoByte(int col) {
            return parquetCursor.getRecord().getGeoByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return parquetCursor.getRecord().getGeoInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return parquetCursor.getRecord().getGeoLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return parquetCursor.getRecord().getGeoShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return parquetCursor.getRecord().getIPv4(col);
        }

        @Override
        public int getInt(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getInt(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? (int) partitionLongValues.getQuick(idx) : Numbers.INT_NULL;
        }

        @Override
        public Interval getInterval(int col) {
            return parquetCursor.getRecord().getInterval(col);
        }

        @Override
        public long getLong(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getLong(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionLongValues.getQuick(idx) : Numbers.LONG_NULL;
        }

        @Override
        public long getLong128Hi(int col) {
            return parquetCursor.getRecord().getLong128Hi(col);
        }

        @Override
        public long getLong128Lo(int col) {
            return parquetCursor.getRecord().getLong128Lo(col);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            parquetCursor.getRecord().getLong256(col, sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return parquetCursor.getRecord().getLong256A(col);
        }

        @Override
        public Long256 getLong256B(int col) {
            return parquetCursor.getRecord().getLong256B(col);
        }

        @Override
        public short getShort(int col) {
            return parquetCursor.getRecord().getShort(col);
        }

        @Override
        public CharSequence getStrA(int col) {
            return parquetCursor.getRecord().getStrA(col);
        }

        @Override
        public CharSequence getStrB(int col) {
            return parquetCursor.getRecord().getStrB(col);
        }

        @Override
        public int getStrLen(int col) {
            return parquetCursor.getRecord().getStrLen(col);
        }

        @Override
        public long getTimestamp(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getTimestamp(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionLongValues.getQuick(idx) : Numbers.LONG_NULL;
        }

        @Override
        public void getVarchar(int col, Utf16Sink utf16Sink) {
            if (col < parquetColumnCount) {
                parquetCursor.getRecord().getVarchar(col, utf16Sink);
                return;
            }
            int idx = col - parquetColumnCount;
            if (partitionValuePresent.get(idx)) {
                Utf8StringSink value = partitionVarcharValues.getQuick(idx);
                Utf8s.utf8ToUtf16(value, utf16Sink);
            }
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getVarcharA(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionVarcharValues.getQuick(idx) : null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getVarcharB(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionVarcharValues.getQuick(idx) : null;
        }

        @Override
        public int getVarcharSize(int col) {
            if (col < parquetColumnCount) {
                return parquetCursor.getRecord().getVarcharSize(col);
            }
            int idx = col - parquetColumnCount;
            return partitionValuePresent.get(idx) ? partitionVarcharValues.getQuick(idx).size() : TableUtils.NULL_LEN;
        }
    }
}
