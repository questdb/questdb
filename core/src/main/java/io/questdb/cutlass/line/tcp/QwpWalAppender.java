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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.ColumnarRowAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Appends decoded QWP v1 table blocks to WAL.
 * <p>
 * This class handles the conversion from QWP v1 columnar format to QuestDB's
 * WAL row-based format. It supports:
 * <ul>
 *   <li>All QWP v1 column types</li>
 *   <li>Nullable columns</li>
 *   <li>Auto column creation</li>
 *   <li>Timestamp column handling</li>
 * </ul>
 */
public class QwpWalAppender implements QuietCloseable {

    private final boolean autoCreateNewColumns;
    private final int maxFileNameLength;
    private final int maxMetadataChangeRetries;

    // Reusable mapping arrays
    private int[] columnIndexMap;  // Maps QWP column index to QuestDB column index
    private int[] columnTypeMap;   // QuestDB column types
    private byte[] qwpTypes;       // QWP type codes for conversion

    // Optional symbol ID cache for performance optimization
    // Maps clientSymbolId → tableSymbolId to avoid string lookups
    private ConnectionSymbolCache symbolCache;

    /**
     * Creates a new WAL appender.
     *
     * @param autoCreateNewColumns whether to auto-create columns that don't exist
     * @param maxFileNameLength    maximum column name length
     */
    public QwpWalAppender(boolean autoCreateNewColumns, int maxFileNameLength, int maxMetadataChangeRetries) {
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.maxFileNameLength = maxFileNameLength;
        this.maxMetadataChangeRetries = maxMetadataChangeRetries;
        this.columnIndexMap = new int[64];
        this.columnTypeMap = new int[64];
        this.qwpTypes = new byte[64];
    }

    /**
     * Maps a QWP v1 type code to QuestDB column type.
     *
     * @param qwpType QWP v1 type code
     * @return QuestDB column type
     */
    public static int mapQwpTypeToQuestDB(int qwpType) {
        return switch (qwpType) {
            case TYPE_BOOLEAN -> ColumnType.BOOLEAN;
            case TYPE_BYTE -> ColumnType.BYTE;
            case TYPE_SHORT -> ColumnType.SHORT;
            case TYPE_CHAR -> ColumnType.CHAR;
            case TYPE_INT -> ColumnType.INT;
            case TYPE_LONG -> ColumnType.LONG;
            case TYPE_FLOAT -> ColumnType.FLOAT;
            case TYPE_DOUBLE -> ColumnType.DOUBLE;
            case TYPE_VARCHAR -> ColumnType.VARCHAR;
            case TYPE_SYMBOL -> ColumnType.SYMBOL;
            case TYPE_TIMESTAMP -> ColumnType.TIMESTAMP;
            case TYPE_TIMESTAMP_NANOS -> ColumnType.TIMESTAMP_NANO;
            case TYPE_DATE -> ColumnType.DATE;
            case TYPE_UUID -> ColumnType.UUID;
            case TYPE_LONG256 -> ColumnType.LONG256;
            case TYPE_GEOHASH -> ColumnType.GEOLONG; // Default to GEOLONG, precision handled separately
            case TYPE_DOUBLE_ARRAY -> ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, false);
            case TYPE_LONG_ARRAY ->
                    throw CairoException.nonCritical().put("long arrays are not supported, only double arrays");
            case TYPE_DECIMAL64 -> ColumnType.DECIMAL64;
            case TYPE_DECIMAL128 -> ColumnType.DECIMAL128;
            case TYPE_DECIMAL256 -> ColumnType.DECIMAL256;
            default -> throw CairoException.nonCritical().put("unknown QWP v1 type: ").put(qwpType);
        };
    }

    /**
     * Appends a table block using the streaming cursor API (zero-allocation).
     * <p>
     * This method processes the table block row-by-row using flyweight cursors,
     * avoiding intermediate object allocations on the hot path.
     *
     * @param securityContext security context for authorization
     * @param tableBlock      streaming table block cursor
     * @param tud             table update details
     * @throws CommitFailedException if commit fails
     * @throws QwpParseException     if parsing fails during cursor iteration
     */
    public void appendToWalStreaming(
            SecurityContext securityContext,
            QwpTableBlockCursor tableBlock,
            TableUpdateDetails tud
    ) throws CommitFailedException, QwpParseException {
        for (int retryCount = 0; !tud.isDropped(); retryCount++) {
            try {
                appendToWalStreaming0(securityContext, tableBlock, tud);
                return;
            } catch (MetadataChangedException e) {
                if (retryCount == maxMetadataChangeRetries) {
                    throw CairoException.nonCritical().put("metadata changed too many times during WAL append");
                }
                tableBlock.resetRowIteration();
            }
        }
    }

    @Override
    public void close() {
        // No resources to clean up - columnar path uses WalColumnarRowAppender's own resources
    }

    /**
     * Sets the symbol cache to use for optimizing symbol lookups.
     * <p>
     * When set, the appender will cache clientSymbolId → tableSymbolId mappings
     * to avoid repeated string lookups for frequently used symbols.
     *
     * @param symbolCache the connection-level symbol cache, or null to disable caching
     */
    public void setSymbolCache(ConnectionSymbolCache symbolCache) {
        this.symbolCache = symbolCache;
    }

    /**
     * Creates a CairoException for unsupported type coercions.
     */
    private static CairoException coercionNotSupportedException(
            byte qwpType, int columnType, QwpTableBlockCursor tableBlock, int col
    ) {
        return CairoException.nonCritical()
                .put("type coercion from ")
                .put(QwpConstants.getTypeName(qwpType))
                .put(" to ")
                .put(ColumnType.nameOf(columnType))
                .put(" is not supported [column=")
                .put(tableBlock.getColumnDef(col).getName())
                .put(']');
    }

    private static int getArrayBatchDimensionality(
            QwpArrayColumnCursor cursor,
            int rowCount,
            QwpTableBlockCursor tableBlock,
            int colIndex
    ) {
        int batchDims = -1;
        cursor.resetRowPosition();
        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            if (cursor.isNull()) {
                continue;
            }

            final int rowDims = cursor.getNDims();
            if (batchDims == -1) {
                batchDims = rowDims;
            } else if (batchDims != rowDims) {
                throw CairoException.nonCritical()
                        .put("array dimensionality mismatch in QWP batch [column=")
                        .put(tableBlock.getColumnDef(colIndex).getName())
                        .put(", expectedDims=")
                        .put(batchDims)
                        .put(", actualDims=")
                        .put(rowDims)
                        .put(']');
            }
        }
        cursor.resetRowPosition();
        return batchDims;
    }

    /**
     * Checks whether a fixed-width wire type can be coerced to the target column type.
     * Only types within the same family are allowed (e.g., integer↔integer, float↔float),
     * plus integer→float/double cross-family coercion.
     * Other cross-family coercions (e.g., UUID→SHORT) are rejected to prevent silent data corruption.
     */
    private static boolean isFixedTypeCoercionAllowed(byte qwpType, int columnType) {
        int colTag = ColumnType.tagOf(columnType);
        return switch (qwpType) {
            case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG -> colTag == ColumnType.BYTE
                    || colTag == ColumnType.SHORT || colTag == ColumnType.INT || colTag == ColumnType.LONG
                    || colTag == ColumnType.FLOAT || colTag == ColumnType.DOUBLE
                    || colTag == ColumnType.DATE;
            case TYPE_FLOAT, TYPE_DOUBLE -> colTag == ColumnType.BYTE
                    || colTag == ColumnType.SHORT || colTag == ColumnType.INT || colTag == ColumnType.LONG
                    || colTag == ColumnType.FLOAT || colTag == ColumnType.DOUBLE;
            case TYPE_UUID -> colTag == ColumnType.UUID;
            case TYPE_DATE -> colTag == ColumnType.DATE;
            case TYPE_LONG256 -> colTag == ColumnType.LONG256;
            default -> false;
        };
    }

    /**
     * Checks whether the QWP wire type is a floating-point type (FLOAT, DOUBLE).
     */
    private static boolean isFloatWireType(byte qwpType) {
        return qwpType == TYPE_FLOAT || qwpType == TYPE_DOUBLE;
    }

    /**
     * Checks whether the QWP wire type is an integer type (BYTE, SHORT, INT, LONG).
     */
    private static boolean isIntegerWireType(byte qwpType) {
        return qwpType == TYPE_BYTE || qwpType == TYPE_SHORT
                || qwpType == TYPE_INT || qwpType == TYPE_LONG;
    }

    /**
     * Maps a QWP v1 type code to QuestDB column type, with cursor access for decimal scale.
     *
     * @param qwpType    QWP v1 type code
     * @param tableBlock table block cursor for accessing decimal scale
     * @param colIndex   column index
     * @return QuestDB column type
     */
    private static int mapQwpTypeToQuestDB(int qwpType, QwpTableBlockCursor tableBlock, int colIndex) {
        // For decimal types, we need to get the scale from the cursor
        switch (qwpType) {
            case TYPE_DECIMAL64 -> {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL64);
                return ColumnType.getDecimalType(ColumnType.DECIMAL64, precision, scale);
            }
            case TYPE_DECIMAL128 -> {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128);
                return ColumnType.getDecimalType(ColumnType.DECIMAL128, precision, scale);
            }
            case TYPE_DECIMAL256 -> {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL256);
                return ColumnType.getDecimalType(ColumnType.DECIMAL256, precision, scale);
            }
            default -> {
                return mapQwpTypeToQuestDB(qwpType);
            }
        }
    }

    private static boolean shouldDeferMissingArrayColumnCreation(byte qwpType, int columnType) {
        return qwpType == TYPE_DOUBLE_ARRAY
                && ColumnType.isArray(columnType)
                && ColumnType.decodeWeakArrayDimensionality(columnType) == -1;
    }

    /**
     * Creates a CairoException for type mismatches.
     */
    private static CairoException typeMismatchException(
            byte qwpType, int columnType, QwpTableBlockCursor tableBlock, int col
    ) {
        return CairoException.nonCritical()
                .put("cannot write ")
                .put(QwpConstants.getTypeName(qwpType))
                .put(" to column [column=")
                .put(tableBlock.getColumnDef(col).getName())
                .put(", type=")
                .put(ColumnType.nameOf(columnType))
                .put(']');
    }

    private static void validateArrayColumnType(
            CharSequence columnName,
            int columnType,
            int batchDims
    ) {
        if (batchDims < 1 || !ColumnType.isArray(columnType)) {
            return;
        }

        final short elemType = ColumnType.decodeArrayElementType(columnType);
        final int strongType = ColumnType.encodeArrayType(elemType, batchDims, false);
        final int existingDims = ColumnType.decodeWeakArrayDimensionality(columnType);
        assert existingDims != -1 : "weak array dimensionality must not be persisted";
        if (existingDims != batchDims) {
            throw CairoException.nonCritical()
                    .put("array dimensionality mismatch [column=")
                    .put(columnName)
                    .put(", expected=")
                    .put(ColumnType.nameOf(columnType))
                    .put(", actual=")
                    .put(ColumnType.nameOf(strongType))
                    .put(']');
        }
    }

    /**
     * Appends a table block using the columnar API for optimized bulk writes.
     * <p>
     * This method processes entire columns at once rather than row-by-row,
     * enabling direct memory copies for compatible column types.
     *
     * @param tableBlock             streaming table block cursor
     * @param walWriter              the WAL writer
     * @param timestampColumnInBlock index of designated timestamp in block, or -1
     * @param columnCount            number of columns
     * @param rowCount               number of rows
     * @param tud                    table update details
     */
    private void appendToWalColumnar(
            QwpTableBlockCursor tableBlock,
            WalWriter walWriter,
            int timestampColumnInBlock,
            int columnCount,
            int rowCount,
            TableUpdateDetails tud,
            long metadataVersion
    ) throws QwpParseException, CommitFailedException, MetadataChangedException {
        ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
        appender.beginColumnarWrite(rowCount);

        try {
            // beginColumnarWrite may trigger a segment roll if the WAL writer
            // detects a pending ALTER TABLE. Re-check metadata version to avoid
            // writing with stale column types (e.g., SYMBOL after conversion to VARCHAR).
            if (walWriter.getMetadataVersion() != metadataVersion) {
                appender.cancelColumnarWrite();
                throw MetadataChangedException.INSTANCE;
            }
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            boolean outOfOrder = false;
            long prevTimestamp = Long.MIN_VALUE;

            // Determine timestamp precision conversion before first pass
            // This ensures min/max/outOfOrder are computed in the same units as stored values
            boolean needsNanosToMicros = false;
            boolean needsMicrosToNanos = false;
            if (timestampColumnInBlock >= 0) {
                byte tsQwpType = qwpTypes[timestampColumnInBlock];
                int tsColumnType = columnTypeMap[timestampColumnInBlock];
                boolean wireIsNanos = (tsQwpType == TYPE_TIMESTAMP_NANOS);
                boolean columnIsNanos = (tsColumnType == ColumnType.TIMESTAMP_NANO);
                boolean wireIsMicros = (tsQwpType == TYPE_TIMESTAMP);
                boolean columnIsMicros = (tsColumnType == ColumnType.TIMESTAMP);
                needsNanosToMicros = wireIsNanos && columnIsMicros;
                needsMicrosToNanos = wireIsMicros && columnIsNanos;
            }

            // First pass: determine min/max timestamps (in column precision, not wire precision)
            // serverTimestamp is used for null designated timestamps (atNow rows in mixed batches)
            long serverTimestamp = Numbers.LONG_NULL;
            if (timestampColumnInBlock >= 0) {
                boolean hasNullTimestamps = false;
                QwpColumnCursor tsCursor = tableBlock.getColumn(timestampColumnInBlock);
                tsCursor.resetRowPosition();
                for (int row = 0; row < rowCount; row++) {
                    tsCursor.advanceRow();
                    if (!tsCursor.isNull()) {
                        long ts;
                        if (tsCursor instanceof QwpTimestampColumnCursor realTsCursor) {
                            ts = realTsCursor.getTimestamp();
                        } else {
                            ts = ((QwpFixedWidthColumnCursor) tsCursor).getTimestamp();
                        }
                        // Apply same precision conversion that will be applied during write
                        if (needsNanosToMicros) {
                            ts = ts / 1000;
                        } else if (needsMicrosToNanos) {
                            if (ts > Long.MAX_VALUE / 1000 || ts < Long.MIN_VALUE / 1000) {
                                throw CairoException.nonCritical()
                                        .put("timestamp overflow converting micros to nanos: ").put(ts);
                            }
                            ts = ts * 1000;
                        }
                        if (ts < minTimestamp) minTimestamp = ts;
                        if (ts > maxTimestamp) maxTimestamp = ts;
                        if (ts < prevTimestamp) outOfOrder = true;
                        prevTimestamp = ts;
                    } else {
                        hasNullTimestamps = true;
                    }
                }
                tsCursor.resetRowPosition();
                // Assign server time for null designated timestamps (mixed at/atNow batches)
                if (hasNullTimestamps) {
                    serverTimestamp = tud.getTimestampDriver().getTicks();
                    boolean hasExplicitTimestamps = minTimestamp != Long.MAX_VALUE;
                    if (serverTimestamp < minTimestamp) minTimestamp = serverTimestamp;
                    if (serverTimestamp > maxTimestamp) maxTimestamp = serverTimestamp;
                    if (hasExplicitTimestamps) {
                        // Mixed explicit and server-assigned timestamps are interleaved
                        // in row order, so the effective sequence is almost certainly
                        // out of order (server time is typically much later than the
                        // explicit timestamps).
                        outOfOrder = true;
                    }
                }
            } else {
                // No designated timestamp in block - all rows get the same server-assigned timestamp.
                long now = tud.getTimestampDriver().getTicks();
                minTimestamp = maxTimestamp = now;
                serverTimestamp = now;
            }

            // Write each column
            for (int col = 0; col < columnCount; col++) {
                int columnIndex = columnIndexMap[col];
                if (columnIndex < 0) {
                    continue;
                }
                int columnType = columnTypeMap[col];
                byte qwpType = qwpTypes[col];
                QwpColumnCursor cursor = tableBlock.getColumn(col);

                if (col == timestampColumnInBlock) {
                    // Designated timestamp column
                    if (cursor instanceof QwpTimestampColumnCursor tsCursor) {
                        // Check for precision mismatch
                        boolean wireIsNanos = (qwpType == TYPE_TIMESTAMP_NANOS);
                        boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);
                        if (wireIsNanos != columnIsNanos || !tsCursor.supportsDirectAccess()) {
                            // Precision mismatch or Gorilla-encoded - must iterate with conversion
                            appender.putTimestampColumnWithConversion(columnIndex, tsCursor, rowCount,
                                    qwpType, columnType, true, serverTimestamp);
                        } else {
                            // Direct access, no conversion needed
                            appender.putTimestampColumn(columnIndex, tsCursor.getValuesAddress(),
                                    tsCursor.getValueCount(), tsCursor.getNullBitmapAddress(),
                                    rowCount, serverTimestamp);
                        }
                    } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                        appender.putTimestampColumn(columnIndex, fixedCursor.getValuesAddress(),
                                fixedCursor.getValueCount(), fixedCursor.getNullBitmapAddress(),
                                rowCount, serverTimestamp);
                    }
                    continue;
                }

                // Regular columns
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.BOOLEAN -> {
                        if (cursor instanceof QwpBooleanColumnCursor boolCursor) {
                            appender.putBooleanColumn(columnIndex, boolCursor, rowCount);
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToBooleanColumn(columnIndex, strCursor, rowCount);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
                         ColumnType.FLOAT, ColumnType.DOUBLE,
                         ColumnType.DATE,
                         ColumnType.UUID, ColumnType.LONG256,
                         ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> {
                        if (cursor instanceof QwpGeoHashColumnCursor geoHashCursor) {
                            appender.putGeoHashColumn(columnIndex, geoHashCursor, rowCount, columnType);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (!isFixedTypeCoercionAllowed(qwpType, columnType)) {
                                throw coercionNotSupportedException(qwpType, columnType, tableBlock, col);
                            }
                            int wireSize = fixedCursor.getValueSize();
                            int columnSize = ColumnType.sizeOf(columnType);
                            int colTag = ColumnType.tagOf(columnType);
                            boolean isIntegerWire = isIntegerWireType(qwpType);
                            boolean isFloatWire = isFloatWireType(qwpType);
                            boolean isFloatTarget = colTag == ColumnType.FLOAT || colTag == ColumnType.DOUBLE;
                            boolean isIntegerTarget = colTag == ColumnType.BYTE || colTag == ColumnType.SHORT
                                    || colTag == ColumnType.INT || colTag == ColumnType.LONG;
                            if (isIntegerWire && (isFloatTarget || wireSize != columnSize)) {
                                // Integer → float/double, integer widening (INT → LONG), or
                                // integer narrowing with overflow check (INT → BYTE, INT → SHORT)
                                appender.putIntegerToNumericColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (isFloatWire && (isIntegerTarget || wireSize != columnSize)) {
                                // Float → integer (with whole-number + range check),
                                // float widening (FLOAT → DOUBLE), or
                                // float narrowing (DOUBLE → FLOAT)
                                appender.putFloatToNumericColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (wireSize == columnSize || columnSize <= 0) {
                                appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                        fixedCursor.getValueCount(), fixedCursor.getValueSize(),
                                        fixedCursor.getNullBitmapAddress(), rowCount);
                            } else {
                                throw coercionNotSupportedException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpBooleanColumnCursor boolCursor) {
                            int colTag = ColumnType.tagOf(columnType);
                            if (colTag == ColumnType.BYTE || colTag == ColumnType.SHORT
                                    || colTag == ColumnType.INT || colTag == ColumnType.LONG
                                    || colTag == ColumnType.FLOAT || colTag == ColumnType.DOUBLE) {
                                appender.putBooleanToNumericColumn(columnIndex, boolCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            int colTag = ColumnType.tagOf(columnType);
                            switch (colTag) {
                                case ColumnType.UUID ->
                                        appender.putStringToUuidColumn(columnIndex, strCursor, rowCount);
                                case ColumnType.LONG256 ->
                                        appender.putStringToLong256Column(columnIndex, strCursor, rowCount);
                                case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG ->
                                        appender.putStringToGeoHashColumn(columnIndex, strCursor, rowCount, columnType);
                                default ->
                                        appender.putStringToNumericColumn(columnIndex, strCursor, rowCount, columnType);
                            }
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.TIMESTAMP -> {
                        // Non-designated timestamp (handles both micros and nanos precision via tagOf)
                        if (cursor instanceof QwpTimestampColumnCursor tsCursor) {
                            // Check for precision mismatch
                            boolean wireIsNanos = (qwpType == TYPE_TIMESTAMP_NANOS);
                            boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);
                            if (wireIsNanos != columnIsNanos || !tsCursor.supportsDirectAccess()) {
                                // Precision mismatch or Gorilla-encoded - must iterate with conversion
                                appender.putTimestampColumnWithConversion(columnIndex, tsCursor, rowCount,
                                        qwpType, columnType, false, Numbers.LONG_NULL);
                            } else {
                                // Direct access, no conversion needed
                                appender.putFixedColumn(columnIndex, tsCursor.getValuesAddress(),
                                        tsCursor.getValueCount(), 8, tsCursor.getNullBitmapAddress(), rowCount);
                            }
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isIntegerWireType(qwpType)) {
                                // Integer → timestamp: raw epoch offset
                                appender.putIntegerToNumericColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else {
                                throw coercionNotSupportedException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToTimestampColumn(columnIndex, strCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.CHAR -> {
                        if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putCharColumn(columnIndex, strCursor, rowCount);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor
                                && qwpType == TYPE_CHAR) {
                            // TYPE_CHAR wire (2 bytes) → CHAR column (2 bytes): direct copy
                            appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                    fixedCursor.getValueCount(), fixedCursor.getValueSize(),
                                    fixedCursor.getNullBitmapAddress(), rowCount);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.VARCHAR -> {
                        if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putVarcharColumn(columnIndex, strCursor, rowCount);
                        } else if (cursor instanceof QwpBooleanColumnCursor boolCursor) {
                            appender.putBooleanToVarcharColumn(columnIndex, boolCursor, rowCount);
                        } else if (cursor instanceof QwpTimestampColumnCursor tsCursor) {
                            appender.putTimestampToVarcharColumn(columnIndex, tsCursor, rowCount, qwpType);
                        } else if (cursor instanceof QwpGeoHashColumnCursor geoCursor) {
                            appender.putGeoHashToVarcharColumn(columnIndex, geoCursor, rowCount);
                        } else if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimalToVarcharColumn(columnIndex, decCursor, rowCount);
                        } else if (cursor instanceof QwpSymbolColumnCursor symCursor) {
                            appender.putSymbolToVarcharColumn(columnIndex, symCursor, rowCount);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isIntegerWireType(qwpType)) {
                                appender.putFixedToVarcharColumn(columnIndex, fixedCursor, rowCount);
                            } else if (isFloatWireType(qwpType)) {
                                appender.putFloatToVarcharColumn(columnIndex, fixedCursor, rowCount);
                            } else {
                                appender.putFixedOtherToVarcharColumn(columnIndex, fixedCursor, rowCount, qwpType);
                            }
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.STRING -> {
                        if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringColumn(columnIndex, strCursor, rowCount);
                        } else if (cursor instanceof QwpBooleanColumnCursor boolCursor) {
                            appender.putBooleanToStringColumn(columnIndex, boolCursor, rowCount);
                        } else if (cursor instanceof QwpTimestampColumnCursor tsCursor) {
                            appender.putTimestampToStringColumn(columnIndex, tsCursor, rowCount, qwpType);
                        } else if (cursor instanceof QwpGeoHashColumnCursor geoCursor) {
                            appender.putGeoHashToStringColumn(columnIndex, geoCursor, rowCount);
                        } else if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimalToStringColumn(columnIndex, decCursor, rowCount);
                        } else if (cursor instanceof QwpSymbolColumnCursor symCursor) {
                            appender.putSymbolToStringColumn(columnIndex, symCursor, rowCount);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isIntegerWireType(qwpType)) {
                                appender.putFixedToStringColumn(columnIndex, fixedCursor, rowCount);
                            } else if (isFloatWireType(qwpType)) {
                                appender.putFloatToStringColumn(columnIndex, fixedCursor, rowCount);
                            } else {
                                appender.putFixedOtherToStringColumn(columnIndex, fixedCursor, rowCount, qwpType);
                            }
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.SYMBOL -> {
                        if (cursor instanceof QwpSymbolColumnCursor symCursor) {
                            if (symbolCache != null && symCursor.isDeltaMode()) {
                                long tableId = tud.getTableToken().getTableId();
                                int initialSymbolCount = walWriter.getSymbolCountWatermark(columnIndex);
                                appender.putSymbolColumn(
                                        columnIndex,
                                        symCursor,
                                        rowCount,
                                        symbolCache,
                                        tableId,
                                        initialSymbolCount
                                );
                            } else {
                                appender.putSymbolColumn(columnIndex, symCursor, rowCount);
                            }
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isIntegerWireType(qwpType)) {
                                appender.putFixedToSymbolColumn(columnIndex, fixedCursor, rowCount);
                            } else if (isFloatWireType(qwpType)) {
                                appender.putFloatToSymbolColumn(columnIndex, fixedCursor, rowCount);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToSymbolColumn(columnIndex, strCursor, rowCount);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32 -> {
                        if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimalToSmallDecimalColumn(columnIndex, decCursor, rowCount, columnType);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isFloatWireType(qwpType)) {
                                appender.putFloatToDecimalColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (isIntegerWireType(qwpType)) {
                                appender.putFixedToSmallDecimalColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToDecimalColumn(columnIndex, strCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.DECIMAL64 -> {
                        if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimal64Column(columnIndex, decCursor, rowCount, columnType);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isFloatWireType(qwpType)) {
                                appender.putFloatToDecimalColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (isIntegerWireType(qwpType)) {
                                appender.putFixedToDecimal64Column(columnIndex, fixedCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToDecimalColumn(columnIndex, strCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.DECIMAL128 -> {
                        if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimal128Column(columnIndex, decCursor, rowCount, columnType);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isFloatWireType(qwpType)) {
                                appender.putFloatToDecimalColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (isIntegerWireType(qwpType)) {
                                appender.putFixedToDecimal128Column(columnIndex, fixedCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToDecimalColumn(columnIndex, strCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    case ColumnType.DECIMAL256 -> {
                        if (cursor instanceof QwpDecimalColumnCursor decCursor) {
                            appender.putDecimal256Column(columnIndex, decCursor, rowCount, columnType);
                        } else if (cursor instanceof QwpFixedWidthColumnCursor fixedCursor) {
                            if (isFloatWireType(qwpType)) {
                                appender.putFloatToDecimalColumn(columnIndex, fixedCursor, rowCount, columnType);
                            } else if (isIntegerWireType(qwpType)) {
                                appender.putFixedToDecimal256Column(columnIndex, fixedCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else if (cursor instanceof QwpStringColumnCursor strCursor) {
                            appender.putStringToDecimalColumn(columnIndex, strCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(qwpType, columnType, tableBlock, col);
                        }
                    }
                    default -> {
                        // Handle array types
                        if (ColumnType.isArray(columnType)) {
                            if (cursor instanceof QwpArrayColumnCursor arrCursor) {
                                appender.putArrayColumn(columnIndex, arrCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(qwpType, columnType, tableBlock, col);
                            }
                        } else {
                            // Unsupported column type - this should not happen as all types are handled
                            throw CairoException.nonCritical()
                                    .put("unsupported column type for columnar write: ")
                                    .put(ColumnType.nameOf(columnType));
                        }
                    }
                }
            }

            // Write server-assigned timestamp if not in block (atNow case)
            if (timestampColumnInBlock < 0) {
                int timestampIndex = walWriter.getMetadata().getTimestampIndex();
                if (timestampIndex >= 0) {
                    walWriter.putServerAssignedTimestampColumnar(rowCount, serverTimestamp);
                }
            }

            appender.endColumnarWrite(minTimestamp, maxTimestamp, outOfOrder);
            tud.commitIfMaxUncommittedRowsCountReached();
        } catch (Throwable t) {
            appender.cancelColumnarWrite();
            // A concurrent ALTER TABLE (e.g., ALTER COLUMN TYPE) can change metadata
            // mid-write, causing failures like "symbol map reader is not available"
            // when a SYMBOL column has been converted to VARCHAR. Detect this by
            // comparing the metadata version and retry with fresh metadata.
            if (walWriter.getMetadataVersion() != metadataVersion) {
                throw MetadataChangedException.INSTANCE;
            }
            throw t;
        }
    }

    private void appendToWalStreaming0(
            SecurityContext securityContext,
            QwpTableBlockCursor tableBlock,
            TableUpdateDetails tud
    ) throws CommitFailedException, MetadataChangedException, QwpParseException {
        int columnCount = tableBlock.getColumnCount();
        int rowCount = tableBlock.getRowCount();

        if (rowCount == 0) {
            return;
        }

        // Ensure mapping arrays are large enough
        if (columnIndexMap.length < columnCount) {
            columnIndexMap = new int[columnCount];
            columnTypeMap = new int[columnCount];
            qwpTypes = new byte[columnCount];
        }

        TableWriterAPI writer = tud.getWriter();
        if (writer == null) {
            throw CairoException.nonCritical()
                    .put("writer is null for table [table=")
                    .put(tud.getTableNameUtf16())
                    .put(']');
        }
        TableRecordMetadata metadata = writer.getMetadata();
        int timestampIndex = tud.getTimestampIndex();
        long metadataVersion = writer.getMetadataVersion();

        // Phase 1: Resolve column indices and create missing columns
        int timestampColumnInBlock = -1;
        for (int i = 0; i < columnCount; i++) {
            QwpColumnDef colDef = tableBlock.getColumnDef(i);
            String columnName = colDef.getName();
            byte colType = colDef.getTypeCode();
            qwpTypes[i] = colType;

            int columnWriterIndex;
            final int batchDims = colType == TYPE_DOUBLE_ARRAY
                    ? getArrayBatchDimensionality(
                    tableBlock.getArrayColumn(i),
                    rowCount,
                    tableBlock,
                    i
            )
                    : Integer.MIN_VALUE;

            if (columnName.isEmpty() && (colType == TYPE_TIMESTAMP || colType == TYPE_TIMESTAMP_NANOS)) {
                if (timestampIndex < 0) {
                    throw CairoException.nonCritical()
                            .put("designated timestamp provided but table has no designated timestamp [table=")
                            .put(tud.getTableNameUtf16())
                            .put(']');
                }
                columnWriterIndex = timestampIndex;
                timestampColumnInBlock = i;
            } else {
                columnWriterIndex = metadata.getColumnIndexQuiet(columnName);

                if (columnWriterIndex < 0) {
                    if (autoCreateNewColumns && TableUtils.isValidColumnName(columnName, maxFileNameLength)) {
                        final int newColumnType;
                        if (colType == TYPE_DOUBLE_ARRAY) {
                            newColumnType = batchDims > 0
                                    ? ColumnType.encodeArrayType(ColumnType.DOUBLE, batchDims)
                                    : ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, false);
                        } else {
                            newColumnType = mapQwpTypeToQuestDB(colDef.getTypeCode(), tableBlock, i);
                        }
                        if (shouldDeferMissingArrayColumnCreation(colType, newColumnType)) {
                            columnWriterIndex = -1;
                        } else {
                            try {
                                securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                                writer.addColumn(columnName, newColumnType, securityContext);
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnName);
                            } catch (CairoException e) {
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnName);
                                if (columnWriterIndex < 0) {
                                    throw e;
                                }
                            }
                        }
                    } else if (!autoCreateNewColumns) {
                        throw CairoException.nonCritical()
                                .put("new columns not allowed [table=")
                                .put(tud.getTableNameUtf16())
                                .put(", column=")
                                .put(columnName)
                                .put(']');
                    } else {
                        throw CairoException.nonCritical()
                                .put("invalid column name [table=")
                                .put(tud.getTableNameUtf16())
                                .put(", column=")
                                .put(columnName)
                                .put(']');
                    }
                }

                if (columnWriterIndex == timestampIndex) {
                    timestampColumnInBlock = i;
                }
            }

            int columnType = columnWriterIndex >= 0 ? metadata.getColumnType(columnWriterIndex) : ColumnType.UNDEFINED;
            if (colType == TYPE_DOUBLE_ARRAY && ColumnType.isArray(columnType)) {
                validateArrayColumnType(columnName, columnType, batchDims);
            }
            columnIndexMap[i] = columnWriterIndex;
            columnTypeMap[i] = columnType;
        }

        // A concurrent ALTER TABLE (e.g., ALTER COLUMN TYPE) can change the
        // writer's metadata version during Phase 1's addColumn calls. When that
        // happens, column types resolved earlier in the loop are stale.
        if (writer.getMetadataVersion() != metadataVersion) {
            throw MetadataChangedException.INSTANCE;
        }

        // Phase 2: Write data - always use columnar path
        // Writer is always a WalWriter in this context (QWP v1 only supports WAL tables)
        WalWriter walWriter = (WalWriter) writer;
        appendToWalColumnar(tableBlock, walWriter, timestampColumnInBlock, columnCount, rowCount, tud, metadataVersion);
    }
}
