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
import io.questdb.cutlass.line.tcp.v4.IlpV4ColumnDef;
import io.questdb.cutlass.line.tcp.v4.IlpV4DecodedColumn;
import io.questdb.cutlass.line.tcp.v4.IlpV4DecodedTableBlock;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

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
public class IlpV4WalAppender {
    private static final Log LOG = LogFactory.getLog(IlpV4WalAppender.class);

    private final boolean autoCreateNewColumns;
    private final int maxFileNameLength;

    // Reusable mapping arrays
    private int[] columnIndexMap;  // Maps ILP column index to QuestDB column index
    private int[] columnTypeMap;   // QuestDB column types

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
    }

    /**
     * Appends a decoded table block to WAL.
     *
     * @param securityContext security context for authorization
     * @param tableBlock      decoded table block
     * @param tud             table update details
     * @throws CommitFailedException if commit fails
     */
    public void appendToWal(SecurityContext securityContext, IlpV4DecodedTableBlock tableBlock,
                            TableUpdateDetails tud) throws CommitFailedException {
        while (!tud.isDropped()) {
            try {
                appendToWal0(securityContext, tableBlock, tud);
                break;
            } catch (MetadataChangedException e) {
                // Retry - metadata changed during processing
            }
        }
    }

    private void appendToWal0(SecurityContext securityContext, IlpV4DecodedTableBlock tableBlock,
                              TableUpdateDetails tud) throws CommitFailedException, MetadataChangedException {
        int columnCount = tableBlock.getColumnCount();
        int rowCount = tableBlock.getRowCount();

        if (rowCount == 0) {
            return; // Nothing to append
        }

        // Ensure mapping arrays are large enough
        if (columnIndexMap.length < columnCount) {
            columnIndexMap = new int[columnCount];
            columnTypeMap = new int[columnCount];
        }

        TableWriterAPI writer = tud.getWriter();
        if (writer == null) {
            throw CairoException.nonCritical()
                    .put("writer is null for table [table=")
                    .put(tud.getTableNameUtf16())
                    .put(", isWal=")
                    .put(tud.isWal())
                    .put(", tudClass=")
                    .put(tud.getClass().getSimpleName())
                    .put(']');
        }
        TableRecordMetadata metadata = writer.getMetadata();
        int timestampIndex = tud.getTimestampIndex();

        // Phase 1: Resolve column indices and create missing columns
        int timestampColumnInBlock = -1;
        for (int i = 0; i < columnCount; i++) {
            IlpV4ColumnDef colDef = tableBlock.getSchema()[i];
            String columnName = colDef.getName();

            int columnWriterIndex;

            // Empty column name with TIMESTAMP type indicates the designated timestamp.
            // This is sent by at() and maps directly to the table's designated timestamp
            // column, regardless of its actual name.
            if (columnName.isEmpty() && colDef.getTypeCode() == TYPE_TIMESTAMP) {
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
                    // Column doesn't exist
                    if (autoCreateNewColumns && TableUtils.isValidColumnName(columnName, maxFileNameLength)) {
                        securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                        try {
                            int newColumnType = mapIlpV4TypeToQuestDB(colDef.getTypeCode());
                            writer.addColumn(columnName, newColumnType, securityContext);
                            columnWriterIndex = metadata.getWriterIndex(metadata.getColumnIndexQuiet(columnName));
                        } catch (CairoException e) {
                            columnWriterIndex = metadata.getColumnIndexQuiet(columnName);
                            if (columnWriterIndex < 0) {
                                throw e;
                            }
                            // Column was added concurrently
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

                // Track if this regular column happens to be the timestamp column
                if (columnWriterIndex == timestampIndex) {
                    timestampColumnInBlock = i;
                }
            }

            int columnType = metadata.getColumnType(columnWriterIndex);
            columnIndexMap[i] = metadata.getWriterIndex(columnWriterIndex);
            columnTypeMap[i] = columnType;
        }

        // Phase 2: Write rows
        IlpV4DecodedColumn timestampColumn = timestampColumnInBlock >= 0 ?
                tableBlock.getColumn(timestampColumnInBlock) : null;

        for (int row = 0; row < rowCount; row++) {
            // Get timestamp for this row
            long timestamp;
            if (timestampColumn != null && !timestampColumn.isNull(row)) {
                timestamp = timestampColumn.getTimestamp(row);
            } else {
                timestamp = tud.getTimestampDriver().getTicks();
            }

            TableWriter.Row r = writer.newRow(timestamp);
            try {
                for (int col = 0; col < columnCount; col++) {
                    int columnIndex = columnIndexMap[col];
                    int columnType = columnTypeMap[col];

                    // Skip timestamp column (already set)
                    if (col == timestampColumnInBlock) {
                        continue;
                    }

                    IlpV4DecodedColumn column = tableBlock.getColumn(col);

                    if (column.isNull(row)) {
                        // NULL value - skip (row already initialized with NULLs)
                        continue;
                    }

                    writeValue(r, columnIndex, columnType, column, row);
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
     * Writes a single value to a row.
     */
    private void writeValue(TableWriter.Row r, int columnIndex, int columnType,
                            IlpV4DecodedColumn column, int row) {
        int ilpType = column.getType();

        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
                r.putBool(columnIndex, column.getBoolean(row));
                break;

            case ColumnType.BYTE:
                r.putByte(columnIndex, column.getByte(row));
                break;

            case ColumnType.SHORT:
                r.putShort(columnIndex, column.getShort(row));
                break;

            case ColumnType.INT:
                r.putInt(columnIndex, column.getInt(row));
                break;

            case ColumnType.LONG:
                r.putLong(columnIndex, column.getLong(row));
                break;

            case ColumnType.FLOAT:
                r.putFloat(columnIndex, column.getFloat(row));
                break;

            case ColumnType.DOUBLE:
                r.putDouble(columnIndex, column.getDouble(row));
                break;

            case ColumnType.DATE:
                r.putDate(columnIndex, column.getDate(row));
                break;

            case ColumnType.TIMESTAMP:
                r.putTimestamp(columnIndex, column.getTimestamp(row));
                break;

            case ColumnType.STRING:
                r.putStr(columnIndex, column.getString(row));
                break;

            case ColumnType.VARCHAR:
                String strValue = column.getString(row);
                if (strValue != null) {
                    r.putVarchar(columnIndex, new io.questdb.std.str.Utf8String(strValue));
                }
                break;

            case ColumnType.SYMBOL:
                r.putSym(columnIndex, column.getSymbol(row));
                break;

            case ColumnType.UUID:
                r.putLong128(columnIndex, column.getUuidLo(row), column.getUuidHi(row));
                break;

            case ColumnType.LONG256:
                r.putLong256(columnIndex,
                        column.getLong256_0(row),
                        column.getLong256_1(row),
                        column.getLong256_2(row),
                        column.getLong256_3(row));
                break;

            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                r.putGeoHash(columnIndex, column.getGeoHash(row));
                break;

            default:
                LOG.error().$("unsupported column type [type=").$(columnType).$(']').$();
                break;
        }
    }

    /**
     * Maps an ILP v4 type code to QuestDB column type.
     *
     * @param ilpType ILP v4 type code
     * @return QuestDB column type
     */
    public static int mapIlpV4TypeToQuestDB(int ilpType) {
        switch (ilpType) {
            case TYPE_BOOLEAN:
                return ColumnType.BOOLEAN;
            case TYPE_BYTE:
                return ColumnType.BYTE;
            case TYPE_SHORT:
                return ColumnType.SHORT;
            case TYPE_INT:
                return ColumnType.INT;
            case TYPE_LONG:
                return ColumnType.LONG;
            case TYPE_FLOAT:
                return ColumnType.FLOAT;
            case TYPE_DOUBLE:
                return ColumnType.DOUBLE;
            case TYPE_STRING:
                return ColumnType.STRING;
            case TYPE_VARCHAR:
                return ColumnType.VARCHAR;
            case TYPE_SYMBOL:
                return ColumnType.SYMBOL;
            case TYPE_TIMESTAMP:
                return ColumnType.TIMESTAMP;
            case TYPE_DATE:
                return ColumnType.DATE;
            case TYPE_UUID:
                return ColumnType.UUID;
            case TYPE_LONG256:
                return ColumnType.LONG256;
            case TYPE_GEOHASH:
                return ColumnType.GEOLONG; // Default to GEOLONG, precision handled separately
            default:
                throw new IllegalArgumentException("Unknown ILP v4 type: " + ilpType);
        }
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
}
