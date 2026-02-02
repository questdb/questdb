/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.ColumnarRowAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.ilpv4.protocol.*;
import io.questdb.std.Decimals;
import io.questdb.std.QuietCloseable;

import static io.questdb.cutlass.ilpv4.protocol.IlpV4Constants.*;

/**
 * Appends decoded ILP v4 table blocks to WAL.
 * <p>
 * This class handles the conversion from ILP v4 columnar format to QuestDB's
 * WAL row-based format. It supports:
 * <ul>
 *   <li>All ILP v4 column types</li>
 *   <li>Nullable columns</li>
 *   <li>Auto column creation</li>
 *   <li>Timestamp column handling</li>
 * </ul>
 */
public class IlpV4WalAppender implements QuietCloseable {

    private final boolean autoCreateNewColumns;
    private final int maxFileNameLength;

    // Reusable mapping arrays
    private int[] columnIndexMap;  // Maps ILP column index to QuestDB column index
    private int[] columnTypeMap;   // QuestDB column types
    private byte[] ilpTypes;       // ILP type codes for conversion

    // Optional symbol ID cache for performance optimization
    // Maps clientSymbolId → tableSymbolId to avoid string lookups
    private ConnectionSymbolCache symbolCache;

    /**
     * Creates a new WAL appender.
     *
     * @param autoCreateNewColumns whether to auto-create columns that don't exist
     * @param maxFileNameLength    maximum column name length
     */
    public IlpV4WalAppender(boolean autoCreateNewColumns, int maxFileNameLength) {
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.maxFileNameLength = maxFileNameLength;
        this.columnIndexMap = new int[64];
        this.columnTypeMap = new int[64];
        this.ilpTypes = new byte[64];
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
     * Returns the current symbol cache, or null if not set.
     */
    public ConnectionSymbolCache getSymbolCache() {
        return symbolCache;
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
     * @throws IlpV4ParseException   if parsing fails during cursor iteration
     */
    public void appendToWalStreaming(SecurityContext securityContext, IlpV4TableBlockCursor tableBlock,
                                     TableUpdateDetails tud) throws CommitFailedException, IlpV4ParseException {
        while (!tud.isDropped()) {
            try {
                appendToWalStreaming0(securityContext, tableBlock, tud);
                break;
            } catch (MetadataChangedException e) {
                // Retry - reset cursor and retry
                tableBlock.resetRowIteration();
            }
        }
    }

    private void appendToWalStreaming0(SecurityContext securityContext, IlpV4TableBlockCursor tableBlock,
                                       TableUpdateDetails tud) throws CommitFailedException, MetadataChangedException, IlpV4ParseException {
        int columnCount = tableBlock.getColumnCount();
        int rowCount = tableBlock.getRowCount();

        if (rowCount == 0) {
            return;
        }

        // Ensure mapping arrays are large enough
        if (columnIndexMap.length < columnCount) {
            columnIndexMap = new int[columnCount];
            columnTypeMap = new int[columnCount];
            ilpTypes = new byte[columnCount];
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

        // Phase 1: Resolve column indices and create missing columns
        int timestampColumnInBlock = -1;
        for (int i = 0; i < columnCount; i++) {
            IlpV4ColumnDef colDef = tableBlock.getColumnDef(i);
            String columnName = colDef.getName();
            byte colType = colDef.getTypeCode();
            ilpTypes[i] = colType;

            int columnWriterIndex;

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
                        securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                        try {
                            int newColumnType = mapIlpV4TypeToQuestDB(colDef.getTypeCode(), tableBlock, i);
                            writer.addColumn(columnName, newColumnType, securityContext);
                            columnWriterIndex = metadata.getColumnIndexQuiet(columnName);
                        } catch (CairoException e) {
                            columnWriterIndex = metadata.getColumnIndexQuiet(columnName);
                            if (columnWriterIndex < 0) {
                                throw e;
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

            int columnType = metadata.getColumnType(columnWriterIndex);
            columnIndexMap[i] = columnWriterIndex;
            columnTypeMap[i] = columnType;
        }

        // Phase 2: Write data - always use columnar path
        // Writer is always a WalWriter in this context (ILP v4 only supports WAL tables)
        WalWriter walWriter = (WalWriter) writer;
        appendToWalColumnar(tableBlock, walWriter, timestampColumnInBlock, columnCount, rowCount, tud);
    }

    /**
     * Appends a table block using the columnar API for optimized bulk writes.
     * <p>
     * This method processes entire columns at once rather than row-by-row,
     * enabling direct memory copies for compatible column types.
     *
     * @param tableBlock          streaming table block cursor
     * @param walWriter           the WAL writer
     * @param timestampColumnInBlock index of designated timestamp in block, or -1
     * @param columnCount         number of columns
     * @param rowCount            number of rows
     * @param tud                 table update details
     */
    private void appendToWalColumnar(IlpV4TableBlockCursor tableBlock, WalWriter walWriter,
                                     int timestampColumnInBlock, int columnCount, int rowCount,
                                     TableUpdateDetails tud) throws IlpV4ParseException, CommitFailedException {
        ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
        appender.beginColumnarWrite(rowCount);

        try {
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            boolean outOfOrder = false;
            long prevTimestamp = Long.MIN_VALUE;

            // Determine timestamp precision conversion before first pass
            // This ensures min/max/outOfOrder are computed in the same units as stored values
            boolean needsNanosToMicros = false;
            boolean needsMicrosToNanos = false;
            if (timestampColumnInBlock >= 0) {
                byte tsIlpType = ilpTypes[timestampColumnInBlock];
                int tsColumnType = columnTypeMap[timestampColumnInBlock];
                boolean wireIsNanos = (tsIlpType == TYPE_TIMESTAMP_NANOS);
                boolean columnIsNanos = (tsColumnType == ColumnType.TIMESTAMP_NANO);
                needsNanosToMicros = wireIsNanos && !columnIsNanos;
                needsMicrosToNanos = !wireIsNanos && columnIsNanos;
            }

            // First pass: determine min/max timestamps (in column precision, not wire precision)
            if (timestampColumnInBlock >= 0) {
                IlpV4ColumnCursor tsCursor = tableBlock.getColumn(timestampColumnInBlock);
                tsCursor.resetRowPosition();
                for (int row = 0; row < rowCount; row++) {
                    tsCursor.advanceRow();
                    if (!tsCursor.isNull()) {
                        long ts;
                        if (tsCursor instanceof IlpV4TimestampColumnCursor) {
                            ts = ((IlpV4TimestampColumnCursor) tsCursor).getTimestamp();
                        } else {
                            ts = ((IlpV4FixedWidthColumnCursor) tsCursor).getTimestamp();
                        }
                        // Apply same precision conversion that will be applied during write
                        if (needsNanosToMicros) {
                            ts = ts / 1000;
                        } else if (needsMicrosToNanos) {
                            ts = ts * 1000;
                        }
                        if (ts < minTimestamp) minTimestamp = ts;
                        if (ts > maxTimestamp) maxTimestamp = ts;
                        if (ts < prevTimestamp) outOfOrder = true;
                        prevTimestamp = ts;
                    }
                }
                tsCursor.resetRowPosition();
            } else {
                // No designated timestamp in block - use current time for min/max estimation.
                // Actual timestamps are assigned per-row in putServerAssignedTimestamp().
                long now = tud.getTimestampDriver().getTicks();
                minTimestamp = maxTimestamp = now;
            }

            // Write each column
            for (int col = 0; col < columnCount; col++) {
                int columnIndex = columnIndexMap[col];
                int columnType = columnTypeMap[col];
                byte ilpType = ilpTypes[col];
                IlpV4ColumnCursor cursor = tableBlock.getColumn(col);

                if (col == timestampColumnInBlock) {
                    // Designated timestamp column
                    if (cursor instanceof IlpV4TimestampColumnCursor tsCursor) {
                        // Check for precision mismatch
                        boolean wireIsNanos = (ilpType == TYPE_TIMESTAMP_NANOS);
                        boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);
                        if (wireIsNanos != columnIsNanos || !tsCursor.supportsDirectAccess()) {
                            // Precision mismatch or Gorilla-encoded - must iterate with conversion
                            appender.putTimestampColumnWithConversion(columnIndex, tsCursor, rowCount,
                                    ilpType, columnType, true, walWriter.getSegmentRowCount());
                        } else {
                            // Direct access, no conversion needed
                            appender.putTimestampColumn(columnIndex, tsCursor.getValuesAddress(),
                                    tsCursor.getValueCount(), tsCursor.getNullBitmapAddress(),
                                    rowCount, walWriter.getSegmentRowCount());
                        }
                    } else if (cursor instanceof IlpV4FixedWidthColumnCursor fixedCursor) {
                        appender.putTimestampColumn(columnIndex, fixedCursor.getValuesAddress(),
                                fixedCursor.getValueCount(), fixedCursor.getNullBitmapAddress(),
                                rowCount, walWriter.getSegmentRowCount());
                    }
                    continue;
                }

                // Regular columns
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.BOOLEAN:
                        if (cursor instanceof IlpV4BooleanColumnCursor boolCursor) {
                            appender.putBooleanColumn(columnIndex, boolCursor, rowCount);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                    case ColumnType.DATE:
                    case ColumnType.UUID:
                    case ColumnType.LONG256:
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
                        if (cursor instanceof IlpV4GeoHashColumnCursor geoHashCursor) {
                            appender.putGeoHashColumn(columnIndex, geoHashCursor, rowCount, columnType);
                        } else if (cursor instanceof IlpV4FixedWidthColumnCursor fixedCursor) {
                            int wireSize = fixedCursor.getValueSize();
                            int columnSize = ColumnType.sizeOf(columnType);
                            if (wireSize != columnSize && columnSize > 0) {
                                // Type narrowing: wire is wider than column (e.g. DOUBLE→FLOAT, LONG→SHORT)
                                appender.putFixedColumnNarrowing(columnIndex, fixedCursor.getValuesAddress(),
                                        fixedCursor.getValueCount(), wireSize,
                                        fixedCursor.getNullBitmapAddress(), rowCount, columnType);
                            } else {
                                appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                        fixedCursor.getValueCount(), fixedCursor.getValueSize(),
                                        fixedCursor.getNullBitmapAddress(), rowCount);
                            }
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.TIMESTAMP:
                        // Non-designated timestamp (handles both micros and nanos precision via tagOf)
                        if (cursor instanceof IlpV4TimestampColumnCursor tsCursor) {
                            // Check for precision mismatch
                            boolean wireIsNanos = (ilpType == TYPE_TIMESTAMP_NANOS);
                            boolean columnIsNanos = (columnType == ColumnType.TIMESTAMP_NANO);
                            if (wireIsNanos != columnIsNanos || !tsCursor.supportsDirectAccess()) {
                                // Precision mismatch or Gorilla-encoded - must iterate with conversion
                                appender.putTimestampColumnWithConversion(columnIndex, tsCursor, rowCount,
                                        ilpType, columnType, false, walWriter.getSegmentRowCount());
                            } else {
                                // Direct access, no conversion needed
                                appender.putFixedColumn(columnIndex, tsCursor.getValuesAddress(),
                                        tsCursor.getValueCount(), 8, tsCursor.getNullBitmapAddress(), rowCount);
                            }
                        } else if (cursor instanceof IlpV4FixedWidthColumnCursor fixedCursor) {
                            appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                    fixedCursor.getValueCount(), 8, fixedCursor.getNullBitmapAddress(), rowCount);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.CHAR:
                        if (cursor instanceof IlpV4StringColumnCursor strCursor) {
                            appender.putCharColumn(columnIndex, strCursor, rowCount);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.VARCHAR:
                        if (cursor instanceof IlpV4StringColumnCursor strCursor) {
                            appender.putVarcharColumn(columnIndex, strCursor, rowCount);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.STRING:
                        if (cursor instanceof IlpV4StringColumnCursor strCursor) {
                            appender.putStringColumn(columnIndex, strCursor, rowCount);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.SYMBOL:
                        if (cursor instanceof IlpV4SymbolColumnCursor symCursor) {
                            if (symbolCache != null && symCursor.isDeltaMode()) {
                                long tableId = tud.getTableToken().getTableId();
                                int initialSymbolCount = walWriter.getSymbolCountWatermark(columnIndex);
                                appender.putSymbolColumn(columnIndex, symCursor, rowCount,
                                                         symbolCache, tableId, initialSymbolCount);
                            } else {
                                appender.putSymbolColumn(columnIndex, symCursor, rowCount);
                            }
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.DECIMAL64:
                        if (cursor instanceof IlpV4DecimalColumnCursor decCursor) {
                            appender.putDecimal64Column(columnIndex, decCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.DECIMAL128:
                        if (cursor instanceof IlpV4DecimalColumnCursor decCursor) {
                            appender.putDecimal128Column(columnIndex, decCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    case ColumnType.DECIMAL256:
                        if (cursor instanceof IlpV4DecimalColumnCursor decCursor) {
                            appender.putDecimal256Column(columnIndex, decCursor, rowCount, columnType);
                        } else {
                            throw typeMismatchException(ilpType, columnType, tableBlock, col);
                        }
                        break;

                    default:
                        // Handle array types
                        if (ColumnType.isArray(columnType)) {
                            if (cursor instanceof IlpV4ArrayColumnCursor arrCursor) {
                                appender.putArrayColumn(columnIndex, arrCursor, rowCount, columnType);
                            } else {
                                throw typeMismatchException(ilpType, columnType, tableBlock, col);
                            }
                        } else {
                            // Unsupported column type - this should not happen as all types are handled
                            throw new UnsupportedOperationException("Unsupported column type for columnar write: "
                                    + ColumnType.nameOf(columnType));
                        }
                }
            }

            // Write server-assigned timestamp if not in block (atNow case)
            if (timestampColumnInBlock < 0) {
                int timestampIndex = walWriter.getMetadata().getTimestampIndex();
                if (timestampIndex >= 0) {
                    putServerAssignedTimestamp(walWriter, timestampIndex, tud, rowCount);
                }
            }

            appender.endColumnarWrite(minTimestamp, maxTimestamp, outOfOrder);
            tud.commitIfMaxUncommittedRowsCountReached();
        } catch (Throwable t) {
            appender.cancelColumnarWrite();
            throw t;
        }
    }

    /**
     * Writes server-assigned timestamp for all rows (atNow case).
     * The designated timestamp uses 128-bit format: (timestamp, rowId) pairs.
     * Each row gets a fresh timestamp from getTicks() to match row-by-row behavior.
     */
    private void putServerAssignedTimestamp(WalWriter walWriter, int columnIndex,
                                            TableUpdateDetails tud, int rowCount) {
        io.questdb.cairo.vm.api.MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        long startRowId = walWriter.getSegmentRowCount();

        for (int row = 0; row < rowCount; row++) {
            long timestamp = tud.getTimestampDriver().getTicks();  // Per-row timestamp
            dataMem.putLong128(timestamp, startRowId + row);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    /**
     * Maps an ILP v4 type code to QuestDB column type, with cursor access for decimal scale.
     *
     * @param ilpType    ILP v4 type code
     * @param tableBlock table block cursor for accessing decimal scale
     * @param colIndex   column index
     * @return QuestDB column type
     */
    private static int mapIlpV4TypeToQuestDB(int ilpType, IlpV4TableBlockCursor tableBlock, int colIndex) {
        // For decimal types, we need to get the scale from the cursor
        switch (ilpType) {
            case TYPE_DECIMAL64: {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL64);
                return ColumnType.getDecimalType(ColumnType.DECIMAL64, precision, scale);
            }
            case TYPE_DECIMAL128: {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128);
                return ColumnType.getDecimalType(ColumnType.DECIMAL128, precision, scale);
            }
            case TYPE_DECIMAL256: {
                int scale = tableBlock.getDecimalColumn(colIndex).getScale() & 0xFF;
                int precision = Decimals.getDecimalTagPrecision(ColumnType.DECIMAL256);
                return ColumnType.getDecimalType(ColumnType.DECIMAL256, precision, scale);
            }
            default:
                return mapIlpV4TypeToQuestDB(ilpType);
        }
    }

    /**
     * Maps an ILP v4 type code to QuestDB column type.
     *
     * @param ilpType ILP v4 type code
     * @return QuestDB column type
     */
    public static int mapIlpV4TypeToQuestDB(int ilpType) {
        return switch (ilpType) {
            case TYPE_BOOLEAN -> ColumnType.BOOLEAN;
            case TYPE_BYTE -> ColumnType.BYTE;
            case TYPE_SHORT -> ColumnType.SHORT;
            case TYPE_CHAR -> ColumnType.CHAR;
            case TYPE_INT -> ColumnType.INT;
            case TYPE_LONG -> ColumnType.LONG;
            case TYPE_FLOAT -> ColumnType.FLOAT;
            case TYPE_DOUBLE -> ColumnType.DOUBLE;
            case TYPE_STRING, TYPE_VARCHAR -> ColumnType.VARCHAR;
            case TYPE_SYMBOL -> ColumnType.SYMBOL;
            case TYPE_TIMESTAMP -> ColumnType.TIMESTAMP;
            case TYPE_TIMESTAMP_NANOS -> ColumnType.TIMESTAMP_NANO;
            case TYPE_DATE -> ColumnType.DATE;
            case TYPE_UUID -> ColumnType.UUID;
            case TYPE_LONG256 -> ColumnType.LONG256;
            case TYPE_GEOHASH -> ColumnType.GEOLONG; // Default to GEOLONG, precision handled separately
            case TYPE_DOUBLE_ARRAY -> ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, false);
            case TYPE_LONG_ARRAY -> throw new IllegalArgumentException("Long arrays are not supported, only double arrays");
            case TYPE_DECIMAL64 -> ColumnType.DECIMAL64;
            case TYPE_DECIMAL128 -> ColumnType.DECIMAL128;
            case TYPE_DECIMAL256 -> ColumnType.DECIMAL256;
            default -> throw new IllegalArgumentException("Unknown ILP v4 type: " + ilpType);
        };
    }

    /**
     * Maps a QuestDB column type to ILP v4 type code.
     *
     * @param columnType QuestDB column type
     * @return ILP v4 type code
     */
    public static byte mapQuestDBTypeToIlpV4(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> TYPE_BOOLEAN;
            case ColumnType.BYTE -> TYPE_BYTE;
            case ColumnType.SHORT -> TYPE_SHORT;
            case ColumnType.INT -> TYPE_INT;
            case ColumnType.LONG -> TYPE_LONG;
            case ColumnType.FLOAT -> TYPE_FLOAT;
            case ColumnType.DOUBLE -> TYPE_DOUBLE;
            case ColumnType.CHAR -> TYPE_CHAR;
            case ColumnType.STRING -> TYPE_STRING;
            case ColumnType.VARCHAR -> TYPE_VARCHAR;
            case ColumnType.SYMBOL -> TYPE_SYMBOL;
            case ColumnType.TIMESTAMP -> TYPE_TIMESTAMP;
            case ColumnType.DATE -> TYPE_DATE;
            case ColumnType.UUID -> TYPE_UUID;
            case ColumnType.LONG256 -> TYPE_LONG256;
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> TYPE_GEOHASH;
            default -> throw new IllegalArgumentException("Unsupported QuestDB type: " + columnType);
        };
    }

    /**
     * Creates a CairoException for type mismatches.
     */
    private static CairoException typeMismatchException(byte ilpType, int columnType,
                                                         IlpV4TableBlockCursor tableBlock, int col) {
        return CairoException.nonCritical()
                .put("cannot write ")
                .put(getIlpTypeName(ilpType))
                .put(" to column [column=")
                .put(tableBlock.getColumnDef(col).getName())
                .put(", type=")
                .put(ColumnType.nameOf(columnType))
                .put(']');
    }

    /**
     * Returns a human-readable name for an ILP v4 type code.
     *
     * @param ilpType ILP v4 type code
     * @return human-readable type name
     */
    private static String getIlpTypeName(byte ilpType) {
        return switch (ilpType & TYPE_MASK) {
            case TYPE_BOOLEAN -> "BOOLEAN";
            case TYPE_BYTE -> "BYTE";
            case TYPE_SHORT -> "SHORT";
            case TYPE_INT -> "INT";
            case TYPE_LONG -> "LONG";
            case TYPE_FLOAT -> "FLOAT";
            case TYPE_DOUBLE -> "DOUBLE";
            case TYPE_STRING -> "STRING";
            case TYPE_VARCHAR -> "VARCHAR";
            case TYPE_SYMBOL -> "SYMBOL";
            case TYPE_TIMESTAMP -> "TIMESTAMP";
            case TYPE_TIMESTAMP_NANOS -> "TIMESTAMP_NANOS";
            case TYPE_DATE -> "DATE";
            case TYPE_UUID -> "UUID";
            case TYPE_LONG256 -> "LONG256";
            case TYPE_GEOHASH -> "GEOHASH";
            case TYPE_DOUBLE_ARRAY -> "DOUBLE_ARRAY";
            case TYPE_LONG_ARRAY -> "LONG_ARRAY";
            case TYPE_DECIMAL64 -> "DECIMAL64";
            case TYPE_DECIMAL128 -> "DECIMAL128";
            case TYPE_DECIMAL256 -> "DECIMAL256";
            default -> "UNKNOWN(" + ilpType + ")";
        };
    }

}
