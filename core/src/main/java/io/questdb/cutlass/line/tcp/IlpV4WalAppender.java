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
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.wal.ColumnarRowAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.ilpv4.protocol.*;
import io.questdb.griffin.DecimalUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;

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
    private static final Log LOG = LogFactory.getLog(IlpV4WalAppender.class);

    private final boolean autoCreateNewColumns;
    private final int maxFileNameLength;

    // Reusable mapping arrays
    private int[] columnIndexMap;  // Maps ILP column index to QuestDB column index
    private int[] columnTypeMap;   // QuestDB column types
    private byte[] ilpTypes;       // ILP type codes for conversion

    // Reusable array for writing array columns
    private final DirectArray reusableArray = new DirectArray();

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
        reusableArray.close();
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

        // Phase 2: Write data - use columnar path for WalWriter, row-by-row for others
        // Check if columnar path can be used:
        // - No array columns (columnar path doesn't support arrays yet)
        // - All ILP types are compatible with column types (type mismatches need row-by-row for proper error handling)
        boolean canUseColumnarPath = true;
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isArray(columnTypeMap[i])) {
                canUseColumnarPath = false;
                break;
            }
            // Check type compatibility - columnar path silently skips mismatched types
            if (!isIlpTypeCompatibleWithColumn(ilpTypes[i], columnTypeMap[i], tableBlock.getColumn(i))) {
                canUseColumnarPath = false;
                break;
            }
        }

        if (writer instanceof WalWriter walWriter && canUseColumnarPath) {
            // Use optimized columnar path for WAL writers
            appendToWalColumnar(tableBlock, walWriter, timestampColumnInBlock, columnCount, rowCount, tud);
            return;
        }

        // Fallback to row-by-row for non-WAL writers
        IlpV4ColumnCursor timestampCursor = timestampColumnInBlock >= 0 ?
                tableBlock.getColumn(timestampColumnInBlock) : null;

        while (tableBlock.hasNextRow()) {
            tableBlock.nextRow();

            // Get timestamp for this row
            long timestamp;
            if (timestampCursor != null && !tableBlock.isColumnNull(timestampColumnInBlock)) {
                timestamp = ((IlpV4TimestampColumnCursor) timestampCursor).getTimestamp();
            } else {
                timestamp = tud.getTimestampDriver().getTicks();
            }

            TableWriter.Row r = writer.newRow(timestamp);
            try {
                for (int col = 0; col < columnCount; col++) {
                    if (col == timestampColumnInBlock) {
                        continue;
                    }

                    if (tableBlock.isColumnNull(col)) {
                        continue;
                    }

                    int columnIndex = columnIndexMap[col];
                    int columnType = columnTypeMap[col];
                    byte ilpType = ilpTypes[col];

                     // Special handling for SYMBOL columns with caching
                     if (ColumnType.tagOf(columnType) == ColumnType.SYMBOL && symbolCache != null) {
                         IlpV4ColumnCursor cursor = tableBlock.getColumn(col);
                         if (cursor instanceof IlpV4SymbolColumnCursor) {
                             long tableToken = writer.getTableToken().getTableId();
                             int watermark = writer.getSymbolCountWatermark(columnIndex);
                             ClientSymbolCache columnCache = symbolCache.getCache(tableToken, columnIndex);
                             columnCache.checkAndInvalidate(watermark);  // Clear cache if watermark changed
                             if (writeSymbolWithCache(columnIndex, (IlpV4SymbolColumnCursor) cursor, r,
                                     writer, tableToken, columnCache, watermark)) {
                                 continue;  // Cache path succeeded
                             }
                             // Cache path couldn't handle it - fall through to writeValueFromCursor
                         }
                     }

                    writeValueFromCursor(r, columnIndex, columnType, ilpType, tableBlock.getColumn(col));
                }
                r.append();
                tud.commitIfMaxUncommittedRowsCountReached();
            } catch (CommitFailedException e) {
                throw e;
            } catch (CairoException e) {
                r.cancel();
                throw e;
            }
        }
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

            // First pass: determine min/max timestamps
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
                    int timestampIndex = walWriter.getMetadata().getTimestampIndex();
                    if (cursor instanceof IlpV4TimestampColumnCursor tsCursor) {
                        if (tsCursor.supportsDirectAccess()) {
                            appender.putTimestampColumn(columnIndex, tsCursor.getValuesAddress(),
                                    tsCursor.getValueCount(), tsCursor.getNullBitmapAddress(),
                                    rowCount, walWriter.getSegmentRowCount());
                        } else {
                            // Gorilla-encoded - must iterate
                            putTimestampColumnIterative(appender, columnIndex, tsCursor, rowCount, walWriter);
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
                        appender.putBooleanColumn(columnIndex, (IlpV4BooleanColumnCursor) cursor, rowCount);
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
                        if (cursor instanceof IlpV4FixedWidthColumnCursor fixedCursor) {
                            appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                    fixedCursor.getValueCount(), fixedCursor.getValueSize(),
                                    fixedCursor.getNullBitmapAddress(), rowCount);
                        }
                        break;

                    case ColumnType.TIMESTAMP:
                        // Non-designated timestamp
                        if (cursor instanceof IlpV4TimestampColumnCursor tsCursor) {
                            if (tsCursor.supportsDirectAccess()) {
                                appender.putFixedColumn(columnIndex, tsCursor.getValuesAddress(),
                                        tsCursor.getValueCount(), 8, tsCursor.getNullBitmapAddress(), rowCount);
                            } else {
                                // Gorilla-encoded - must iterate
                                putTimestampColumnAsFixed(columnIndex, tsCursor, rowCount, walWriter);
                            }
                        } else if (cursor instanceof IlpV4FixedWidthColumnCursor fixedCursor) {
                            appender.putFixedColumn(columnIndex, fixedCursor.getValuesAddress(),
                                    fixedCursor.getValueCount(), 8, fixedCursor.getNullBitmapAddress(), rowCount);
                        }
                        break;

                    case ColumnType.VARCHAR:
                        appender.putVarcharColumn(columnIndex, (IlpV4StringColumnCursor) cursor, rowCount);
                        break;

                    case ColumnType.STRING:
                        appender.putStringColumn(columnIndex, (IlpV4StringColumnCursor) cursor, rowCount);
                        break;

                    case ColumnType.SYMBOL:
                        appender.putSymbolColumn(columnIndex, (IlpV4SymbolColumnCursor) cursor, rowCount);
                        break;

                    default:
                        // Unsupported column type for columnar path - this should not happen
                        // as canUseColumnarPath should have detected this and fallen back to row-by-row
                        throw new UnsupportedOperationException("Unsupported column type for columnar write: "
                                + ColumnType.nameOf(columnType));
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
     * Writes a timestamp column iteratively (for Gorilla-encoded data).
     */
    private void putTimestampColumnIterative(ColumnarRowAppender appender, int columnIndex,
                                              IlpV4TimestampColumnCursor cursor, int rowCount,
                                              WalWriter walWriter) throws IlpV4ParseException {
        // For Gorilla-encoded timestamps, we need to iterate
        // This path delegates to the standard mechanism since we can't bulk copy
        cursor.resetRowPosition();
        io.questdb.cairo.vm.api.MemoryMA dataMem = walWriter.getDataColumn(columnIndex);
        long startRowId = walWriter.getSegmentRowCount();

        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            long timestamp = cursor.isNull() ? io.questdb.std.Numbers.LONG_NULL : cursor.getTimestamp();
            dataMem.putLong128(timestamp, startRowId + row);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, startRowId + rowCount - 1);
    }

    /**
     * Writes a non-designated timestamp column iteratively (for Gorilla-encoded data).
     * Non-designated timestamps are stored as simple 8-byte longs (not 128-bit like designated timestamps).
     */
    private void putTimestampColumnAsFixed(int columnIndex, IlpV4TimestampColumnCursor cursor,
                                           int rowCount, WalWriter walWriter) throws IlpV4ParseException {
        cursor.resetRowPosition();
        io.questdb.cairo.vm.api.MemoryMA dataMem = walWriter.getDataColumn(columnIndex);

        for (int row = 0; row < rowCount; row++) {
            cursor.advanceRow();
            long timestamp = cursor.isNull() ? io.questdb.std.Numbers.LONG_NULL : cursor.getTimestamp();
            dataMem.putLong(timestamp);
        }
        walWriter.setRowValueNotNullColumnar(columnIndex, walWriter.getSegmentRowCount() + rowCount - 1);
    }

    /**
     * Writes a value from a cursor to a row (zero-allocation for most types).
     */
    private void writeValueFromCursor(TableWriter.Row r, int columnIndex, int columnType,
                                      byte ilpType, IlpV4ColumnCursor cursor) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
                r.putBool(columnIndex, ((IlpV4BooleanColumnCursor) cursor).getValue());
                break;

            case ColumnType.BYTE:
                r.putByte(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getByte());
                break;

            case ColumnType.SHORT:
                r.putShort(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getShort());
                break;

            case ColumnType.INT:
                r.putInt(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getInt());
                break;

            case ColumnType.LONG:
                r.putLong(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getLong());
                break;

            case ColumnType.FLOAT:
                r.putFloat(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getFloat());
                break;

            case ColumnType.DOUBLE:
                r.putDouble(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getDouble());
                break;

            case ColumnType.DATE:
                r.putDate(columnIndex, ((IlpV4FixedWidthColumnCursor) cursor).getDate());
                break;

            case ColumnType.TIMESTAMP:
                long tsValue;
                if (cursor instanceof IlpV4TimestampColumnCursor) {
                    tsValue = ((IlpV4TimestampColumnCursor) cursor).getTimestamp();
                } else {
                    tsValue = ((IlpV4FixedWidthColumnCursor) cursor).getTimestamp();
                }
                if (ilpType == TYPE_TIMESTAMP_NANOS && columnType == ColumnType.TIMESTAMP) {
                    tsValue = tsValue / 1000;
                } else if (ilpType == TYPE_TIMESTAMP && columnType == ColumnType.TIMESTAMP_NANO) {
                    tsValue = tsValue * 1000;
                }
                r.putTimestamp(columnIndex, tsValue);
                break;

            case ColumnType.STRING:
                // Note: This allocates for STRING type - unavoidable with current API
                r.putStr(columnIndex, ((IlpV4StringColumnCursor) cursor).getStringValue());
                break;

            case ColumnType.VARCHAR:
                // Zero-allocation! Use DirectUtf8Sequence directly
                DirectUtf8Sequence utf8Value = ((IlpV4StringColumnCursor) cursor).getUtf8Value();
                r.putVarchar(columnIndex, utf8Value);
                break;

            case ColumnType.SYMBOL:
                // May allocate on first access per dictionary entry, then cached
                r.putSym(columnIndex, ((IlpV4SymbolColumnCursor) cursor).getSymbolString());
                break;

            case ColumnType.UUID:
                IlpV4FixedWidthColumnCursor uuidCursor = (IlpV4FixedWidthColumnCursor) cursor;
                r.putLong128(columnIndex, uuidCursor.getUuidLo(), uuidCursor.getUuidHi());
                break;

            case ColumnType.LONG256:
                IlpV4FixedWidthColumnCursor l256Cursor = (IlpV4FixedWidthColumnCursor) cursor;
                r.putLong256(columnIndex,
                        l256Cursor.getLong256_0(),
                        l256Cursor.getLong256_1(),
                        l256Cursor.getLong256_2(),
                        l256Cursor.getLong256_3());
                break;

            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                r.putGeoHash(columnIndex, ((IlpV4GeoHashColumnCursor) cursor).getGeoHash());
                break;

            case ColumnType.DECIMAL64: {
                IlpV4DecimalColumnCursor decCursor = (IlpV4DecimalColumnCursor) cursor;
                int wireScale = decCursor.getScale() & 0xFF;
                int columnScale = ColumnType.getDecimalScale(columnType);
                if (wireScale == columnScale) {
                    // Scales match - write directly without conversion
                    r.putLong(columnIndex, decCursor.getDecimal64());
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(decCursor.getDecimal64());
                    decimal.setScale(wireScale);
                    decimal.rescale(columnScale);
                    DecimalUtil.store(decimal, r, columnIndex, columnType);
                }
                break;
            }

            case ColumnType.DECIMAL128: {
                IlpV4DecimalColumnCursor decCursor = (IlpV4DecimalColumnCursor) cursor;
                int wireScale = decCursor.getScale() & 0xFF;
                int columnScale = ColumnType.getDecimalScale(columnType);
                if (wireScale == columnScale) {
                    // Scales match - write directly without conversion
                    r.putDecimal128(columnIndex, decCursor.getDecimal128Hi(), decCursor.getDecimal128Lo());
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(decCursor.getDecimal128Hi(), decCursor.getDecimal128Lo());
                    decimal.setScale(wireScale);
                    decimal.rescale(columnScale);
                    DecimalUtil.store(decimal, r, columnIndex, columnType);
                }
                break;
            }

            case ColumnType.DECIMAL256: {
                IlpV4DecimalColumnCursor decCursor = (IlpV4DecimalColumnCursor) cursor;
                int wireScale = decCursor.getScale() & 0xFF;
                int columnScale = ColumnType.getDecimalScale(columnType);
                if (wireScale == columnScale) {
                    // Scales match - write directly without conversion
                    r.putDecimal256(columnIndex,
                            decCursor.getDecimal256Hh(),
                            decCursor.getDecimal256Hl(),
                            decCursor.getDecimal256Lh(),
                            decCursor.getDecimal256Ll());
                } else {
                    Decimal256 decimal = Misc.getThreadLocalDecimal256();
                    decimal.ofRaw(
                            decCursor.getDecimal256Hh(),
                            decCursor.getDecimal256Hl(),
                            decCursor.getDecimal256Lh(),
                            decCursor.getDecimal256Ll()
                    );
                    decimal.setScale(wireScale);
                    decimal.rescale(columnScale);
                    DecimalUtil.store(decimal, r, columnIndex, columnType);
                }
                break;
            }

            default:
                if (ColumnType.isArray(columnType)) {
                    writeArrayFromCursor(r, columnIndex, columnType, ilpType, (IlpV4ArrayColumnCursor) cursor);
                } else {
                    LOG.error().$("unsupported column type [type=").$(columnType).$(']').$();
                }
                break;
        }
    }

    /**
     * Checks if the ILP type is compatible with the column type for the columnar path.
     * The columnar path uses instanceof checks that silently skip mismatched types,
     * so we need to detect mismatches upfront and fall back to row-by-row for proper error handling.
     */
    private boolean isIlpTypeCompatibleWithColumn(byte ilpType, int columnType, IlpV4ColumnCursor cursor) {
        int columnTag = ColumnType.tagOf(columnType);

        switch (columnTag) {
            case ColumnType.BOOLEAN:
                return cursor instanceof IlpV4BooleanColumnCursor;

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
                return cursor instanceof IlpV4FixedWidthColumnCursor;

            case ColumnType.TIMESTAMP:
                // Check for precision mismatch - columnar path cannot convert between nanos and micros
                // Row-by-row path handles this conversion in writeValueFromCursor()
                boolean isNanoColumn = columnType == ColumnType.TIMESTAMP_NANO;
                boolean isNanoIlp = ilpType == TYPE_TIMESTAMP_NANOS;
                if (isNanoColumn != isNanoIlp) {
                    // Precision mismatch - force row-by-row path for conversion
                    return false;
                }
                if (cursor instanceof IlpV4TimestampColumnCursor tsCursor) {
                    return tsCursor.supportsDirectAccess();
                }
                return cursor instanceof IlpV4FixedWidthColumnCursor;

            case ColumnType.VARCHAR:
            case ColumnType.STRING:
                return cursor instanceof IlpV4StringColumnCursor;

            case ColumnType.SYMBOL:
                return cursor instanceof IlpV4SymbolColumnCursor;

            default:
                // Unknown types - let row-by-row path handle it
                return false;
        }
    }

    /**
     * Writes an array value from cursor to a row.
     */
    private void writeArrayFromCursor(TableWriter.Row r, int columnIndex, int columnType,
                                       byte ilpType, IlpV4ArrayColumnCursor cursor) {
        int nDims = cursor.getNDims();
        int totalElements = cursor.getTotalElements();

        // Determine element type
        short elemType = cursor.isDoubleArray() ? ColumnType.DOUBLE : ColumnType.LONG;
        int encodedType = ColumnType.encodeArrayType(elemType, nDims);

        // Set up the reusable array with the correct type and shape
        reusableArray.setType(encodedType);
        for (int d = 0; d < nDims; d++) {
            reusableArray.setDimLen(d, cursor.getDimSize(d));
        }
        reusableArray.applyShape();

        // Copy data from cursor to reusable array
        MemoryA mem = reusableArray.startMemoryA();
        long srcAddr = cursor.getValuesAddress();
        if (cursor.isDoubleArray()) {
            for (int i = 0; i < totalElements; i++) {
                mem.putDouble(Unsafe.getUnsafe().getDouble(srcAddr + (long) i * 8));
            }
        } else {
            for (int i = 0; i < totalElements; i++) {
                mem.putLong(Unsafe.getUnsafe().getLong(srcAddr + (long) i * 8));
            }
        }

        r.putArray(columnIndex, reusableArray);
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
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
                return TYPE_BOOLEAN;
            case ColumnType.BYTE:
                return TYPE_BYTE;
            case ColumnType.SHORT:
                return TYPE_SHORT;
            case ColumnType.INT:
                return TYPE_INT;
            case ColumnType.LONG:
                return TYPE_LONG;
            case ColumnType.FLOAT:
                return TYPE_FLOAT;
            case ColumnType.DOUBLE:
                return TYPE_DOUBLE;
            case ColumnType.STRING:
                return TYPE_STRING;
            case ColumnType.VARCHAR:
                return TYPE_VARCHAR;
            case ColumnType.SYMBOL:
                return TYPE_SYMBOL;
            case ColumnType.TIMESTAMP:
                return TYPE_TIMESTAMP;
            case ColumnType.DATE:
                return TYPE_DATE;
            case ColumnType.UUID:
                return TYPE_UUID;
            case ColumnType.LONG256:
                return TYPE_LONG256;
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                return TYPE_GEOHASH;
            default:
                throw new IllegalArgumentException("Unsupported QuestDB type: " + columnType);
        }
    }

    /**
     * Writes a symbol value using the cache for optimization.
     * <p>
     * This method attempts to use a cached clientSymbolId → tableSymbolId mapping
     * to avoid string lookups for frequently used symbols.
     * <p>
     * The caching strategy is:
     * <ul>
     *   <li>Cache hit + tableId &lt; watermark: use putSymIndex (committed symbol)</li>
     *   <li>Cache miss: lookup via SymbolMapReader if available, cache if found, then write</li>
     *   <li>New symbol (not in committed table): use putSym, don't cache (local ID)</li>
     * </ul>
     *
     * @param columnIndex    the table column index
     * @param cursor         the symbol column cursor
     * @param r              the row to write to
     * @param writer         the table writer
     * @param tableToken     the table's unique token
     * @param columnCache    the cache for this (table, column) pair
     * @param watermark      the current symbol count watermark (committed symbol count)
     * @return true if the symbol was written successfully, false if the caller should
     *         fall back to the standard write path
     */
    private boolean writeSymbolWithCache(
            int columnIndex,
            IlpV4SymbolColumnCursor cursor,
            TableWriter.Row r,
            TableWriterAPI writer,
            long tableToken,
            ClientSymbolCache columnCache,
            int watermark
    ) {
        int clientSymbolId = cursor.getSymbolIndex();
        if (clientSymbolId < 0) {
            // NULL symbol - nothing to write, but we handled it
            return true;
        }

        // Try cache first (maps clientSymbolId -> tableSymbolId)
        // Only use cached committed symbols (< watermark). Local symbols are not cached because
        // segment rollover resets local IDs without necessarily changing the watermark.
        int cachedTableId = columnCache.get(clientSymbolId);
        if (cachedTableId != ClientSymbolCache.NO_ENTRY && cachedTableId < watermark) {
            // Cache hit for committed symbol - use putSymIndex (fast path)
            r.putSymIndex(columnIndex, cachedTableId);
            if (symbolCache != null) {
                symbolCache.recordHit();
            }
            return true;
        }

        // Cache miss - need to do lookup
        if (symbolCache != null) {
            symbolCache.recordMiss();
        }

        String symbolValue = cursor.getSymbolString();
        if (symbolValue == null) {
            // clientSymbolId >= 0 but getSymbolString() returned null
            // This could happen if the dictionary lookup failed in delta mode.
            // Return false so caller can fall back to the standard write path.
            // Note: This is different from a genuine NULL symbol, which is
            // handled above (clientSymbolId < 0).
            return false;
        }

        // Try to lookup in WalWriter's internal symbol cache
        // This cache is always up-to-date (unlike SymbolMapReader which can be stale)
        if (writer instanceof WalWriter walWriter) {
            int tableId = walWriter.getCachedSymbolKey(columnIndex, symbolValue);
            if (tableId != SymbolTable.VALUE_NOT_FOUND) {
                // Only cache committed symbols (< watermark)
                // Local symbols may be reassigned after segment rollover
                if (tableId < watermark) {
                    columnCache.put(clientSymbolId, tableId);
                }
                r.putSymIndex(columnIndex, tableId);
                return true;
            }
        }

        // Not found - use putSym which will assign a new local ID
        r.putSym(columnIndex, symbolValue);
        // Can't cache - putSym doesn't return the assigned ID
        return true;
    }
}
