/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cutlass.line.LineTcpTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;

class LineTcpMeasurementEvent implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementEvent.class);
    private final boolean autoCreateNewColumns;
    private final LineTcpEventBuffer buffer;
    private final MicrosecondClock clock;
    private final DefaultColumnTypes defaultColumnTypes;
    private final int maxColumnNameLength;
    private final PrincipalOnlySecurityContext principalOnlySecurityContext = new PrincipalOnlySecurityContext();
    private final boolean stringToCharCastAllowed;
    private final LineTcpTimestampAdapter timestampAdapter;
    private boolean commitOnWriterClose;
    private TableUpdateDetails tableUpdateDetails;
    private int writerWorkerId;

    LineTcpMeasurementEvent(
            long bufLo,
            long bufSize,
            MicrosecondClock clock,
            LineTcpTimestampAdapter timestampAdapter,
            DefaultColumnTypes defaultColumnTypes,
            boolean stringToCharCastAllowed,
            int maxColumnNameLength,
            boolean autoCreateNewColumns
    ) {
        this.maxColumnNameLength = maxColumnNameLength;
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.buffer = new LineTcpEventBuffer(bufLo, bufSize);
        this.clock = clock;
        this.timestampAdapter = timestampAdapter;
        this.defaultColumnTypes = defaultColumnTypes;
        this.stringToCharCastAllowed = stringToCharCastAllowed;
    }

    public static CairoException boundsError(long entityValue, int columnIndex, int colType) {
        return CairoException.critical(0)
                .put("line protocol integer is out of ").put(ColumnType.nameOf(colType))
                .put(" bounds [columnIndex=").put(columnIndex)
                .put(", value=").put(entityValue)
                .put(']');
    }

    public static CairoException castError(String ilpType, int columnWriterIndex, int colType, DirectUtf8Sequence name) {
        return CairoException.critical(0)
                .put("cast error for line protocol ").put(ilpType)
                .put(" [columnWriterIndex=").put(columnWriterIndex)
                .put(", columnType=").put(ColumnType.nameOf(colType))
                .put(":").put(colType)
                .put(", name=").put(name)
                .put(']');
    }

    public static CairoException invalidColNameError(TableUpdateDetails tud, CharSequence colName) {
        return CairoException.critical(0)
                .put("invalid column name [table=").put(tud.getTableNameUtf16())
                .put(", columnName=").put(colName)
                .put(']');
    }

    public static CairoException newColumnsNotAllowed(TableUpdateDetails tud, String colName) {
        return CairoException.critical(0)
                .put("column does not exist, creating new columns is disabled [table=").put(tud.getTableNameUtf16())
                .put(", columnName=").put(colName)
                .put(']');
    }

    @Override
    public void close() {
        // this is concurrent writer release
        tableUpdateDetails = Misc.free(tableUpdateDetails);
    }

    public TableUpdateDetails getTableUpdateDetails() {
        return tableUpdateDetails;
    }

    public int getWriterWorkerId() {
        return writerWorkerId;
    }

    public void releaseWriter() {
        tableUpdateDetails.releaseWriter(commitOnWriterClose);
    }

    void append() throws CommitFailedException {
        TableWriter.Row row = null;
        try {
            final TableWriterAPI writer = tableUpdateDetails.getWriter();
            long offset = buffer.getAddress();
            final long metadataVersion = buffer.readLong(offset);
            offset += Long.BYTES;
            if (metadataVersion > writer.getMetadataVersion()) {
                // I/O thread has a more recent version of the WAL table metadata than the writer.
                // Let the WAL writer commit, so that it refreshes its metadata copy.
                // TODO: this method is not used for WAL tables, check if the below commit still needed
                writer.commit();
            }
            long timestamp = buffer.readLong(offset);
            offset += Long.BYTES;
            if (timestamp == LineTcpParser.NULL_TIMESTAMP) {
                timestamp = clock.getTicks();
            }
            row = writer.newRow(timestamp);
            final int nEntities = buffer.readInt(offset);
            offset += Integer.BYTES;
            final long writerMetadataVersion = writer.getMetadataVersion();
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                int colIndex = buffer.readInt(offset);
                offset += Integer.BYTES;
                final byte entityType;
                if (colIndex > -1) {
                    entityType = buffer.readByte(offset);
                    offset += Byte.BYTES;
                    // Did the I/O thread have the latest structure version when it serialized the row?
                    if (metadataVersion < writerMetadataVersion) {
                        // Nope. For WAL tables, it could mean that the column is already dropped. Let's check it.
                        if (!writer.getMetadata().hasColumn(colIndex)) {
                            // The column was dropped, so we skip it.
                            offset += buffer.columnValueLength(entityType, offset);
                            continue;
                        }
                    }
                } else {
                    // Column is passed by name, it is possible that
                    // column is new and has to be added. It is also possible that column
                    // already exist but the publisher is a little out of date and does not yet
                    // have column index.

                    // Column name will be UTF16 encoded already
                    final CharSequence columnName = buffer.readUtf16Chars(offset, -colIndex);
                    offset += -colIndex * 2L;
                    // Column name is followed with the principal name.
                    int principalLen = buffer.readInt(offset);
                    offset += Integer.BYTES;
                    final CharSequence principal = buffer.readUtf16CharsB(offset, principalLen);
                    offset += principalLen * 2L;

                    entityType = buffer.readByte(offset);
                    offset += Byte.BYTES;
                    colIndex = writer.getMetadata().getColumnIndexQuiet(columnName);
                    if (colIndex < 0) {
                        // we have to cancel "active" row to avoid writer committing when
                        // column is added
                        row.cancel();
                        row = null;
                        final int colType = defaultColumnTypes.MAPPED_COLUMN_TYPES[entityType];
                        // we have to commit before adding a new column as WalWriter doesn't do that automatically
                        // TODO: this method is not used for WAL tables, check if the below commit still needed
                        writer.commit();
                        try {
                            writer.addColumn(columnName, colType, principalOnlySecurityContext.of(principal));
                        } catch (CairoException e) {
                            colIndex = writer.getMetadata().getColumnIndexQuiet(columnName);
                            if (colIndex < 0) {
                                // the column is still not there, something must be wrong
                                throw e;
                            }
                            // all good, someone added the column concurrently
                        }

                        // Seek to beginning of entities
                        offset = buffer.getAddressAfterHeader();
                        nEntity = -1;
                        row = writer.newRow(timestamp);
                        continue;
                    }
                }

                CharSequence cs;
                switch (entityType) {
                    case LineTcpParser.ENTITY_TYPE_TAG:
                        cs = buffer.readUtf16Chars(offset);
                        row.putSym(colIndex, cs);
                        offset += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_CACHED_TAG:
                        row.putSymIndex(colIndex, buffer.readInt(offset));
                        offset += Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_LONG:
                    case LineTcpParser.ENTITY_TYPE_GEOLONG:
                        row.putLong(colIndex, buffer.readLong(offset));
                        offset += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_INTEGER:
                    case LineTcpParser.ENTITY_TYPE_GEOINT:
                        row.putInt(colIndex, buffer.readInt(offset));
                        offset += Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_SHORT:
                    case LineTcpParser.ENTITY_TYPE_GEOSHORT:
                        row.putShort(colIndex, buffer.readShort(offset));
                        offset += Short.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_BYTE:
                    case LineTcpParser.ENTITY_TYPE_GEOBYTE:
                        row.putByte(colIndex, buffer.readByte(offset));
                        offset += Byte.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_DATE:
                        row.putDate(colIndex, buffer.readLong(offset));
                        offset += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_DOUBLE:
                        row.putDouble(colIndex, buffer.readDouble(offset));
                        offset += Double.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_FLOAT:
                        row.putFloat(colIndex, buffer.readFloat(offset));
                        offset += Float.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN:
                        row.putBool(colIndex, buffer.readByte(offset) == 1);
                        offset += Byte.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_STRING:
                        cs = buffer.readUtf16Chars(offset);
                        row.putStr(colIndex, cs);
                        offset += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_CHAR:
                        row.putChar(colIndex, buffer.readChar(offset));
                        offset += Character.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_LONG256:
                        cs = buffer.readUtf16Chars(offset);
                        row.putLong256(colIndex, cs);
                        offset += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP:
                        row.putTimestamp(colIndex, buffer.readLong(offset));
                        offset += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_UUID:
                        row.putLong128(colIndex, buffer.readLong(offset), buffer.readLong(offset + Long.BYTES));
                        offset += Long.BYTES * 2;
                        break;
                    case ENTITY_TYPE_NULL:
                        // ignored, default nulls is used
                        break;
                    default:
                        throw new UnsupportedOperationException("entityType " + entityType + " is not implemented!");
                }
            }
            row.append();
            tableUpdateDetails.commitIfMaxUncommittedRowsCountReached();
        } catch (CommitFailedException commitFailedException) {
            throw commitFailedException;
        } catch (Throwable th) {
            LOG.error()
                    .$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.getTableToken())
                    .$(", message=").$(th.getMessage())
                    .$(th)
                    .I$();
            if (row != null) {
                row.cancel();
            }
        }
    }

    void createMeasurementEvent(
            SecurityContext securityContext,
            TableUpdateDetails tud,
            LineTcpParser parser,
            int workerId
    ) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_INCOMPLETE_EVENT;
        final TableUpdateDetails.ThreadLocalDetails localDetails = tud.getThreadLocalDetails(workerId);
        localDetails.resetStateIfNecessary();
        tableUpdateDetails = tud;
        securityContext.authorizeInsert(tud.getTableToken());
        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp, parser.getTimestampUnit());
        }
        buffer.addStructureVersion(buffer.getAddress(), localDetails.getMetadataVersion());
        // timestamp, entitiesWritten are written to the buffer after saving all fields
        // because their values are worked out while the columns are processed
        long offset = buffer.getAddressAfterHeader();
        int entitiesWritten = 0;
        for (int nEntity = 0, n = parser.getEntityCount(); nEntity < n; nEntity++) {
            LineTcpParser.ProtoEntity entity = parser.getEntity(nEntity);
            byte entityType = entity.getType();
            int colType;
            int columnWriterIndex = localDetails.getColumnWriterIndex(entity.getName(), parser.hasNonAsciiChars());
            if (columnWriterIndex > -1) {
                // column index found, processing column by index
                if (columnWriterIndex == tud.getTimestampIndex()) {
                    timestamp = timestampAdapter.getMicros(entity.getLongValue(), entity.getUnit());
                    continue;
                }

                offset = buffer.addColumnIndex(offset, columnWriterIndex);
                colType = localDetails.getColumnType(columnWriterIndex);
            } else if (columnWriterIndex == COLUMN_NOT_FOUND) {
                // send column by name
                final String colNameUtf16 = localDetails.getColNameUtf16();
                if (autoCreateNewColumns && TableUtils.isValidColumnName(colNameUtf16, maxColumnNameLength)) {
                    securityContext.authorizeAlterTableAddColumn(tud.getTableToken());
                    offset = buffer.addColumnName(offset, colNameUtf16, securityContext.getPrincipal());
                    colType = localDetails.getColumnType(localDetails.getColNameUtf8(), entityType);
                } else if (!autoCreateNewColumns) {
                    throw newColumnsNotAllowed(tableUpdateDetails, colNameUtf16);
                } else {
                    throw invalidColNameError(tableUpdateDetails, colNameUtf16);
                }
            } else {
                // duplicate column, skip
                // we could set a boolean in the config if we want to throw exception instead
                continue;
            }

            entitiesWritten++;
            switch (entityType) {
                case LineTcpParser.ENTITY_TYPE_TAG: {
                    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        offset = buffer.addSymbol(
                                offset,
                                entity.getValue(),
                                parser.hasNonAsciiChars(),
                                localDetails.getSymbolLookup(columnWriterIndex)
                        );
                    } else {
                        throw castError("tag", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_INTEGER: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG:
                            offset = buffer.addLong(offset, entity.getLongValue());
                            break;
                        case ColumnType.INT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Integer.MIN_VALUE && entityValue <= Integer.MAX_VALUE) {
                                offset = buffer.addInt(offset, (int) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addInt(offset, Numbers.INT_NaN);
                            } else {
                                throw boundsError(entityValue, nEntity, ColumnType.INT);
                            }
                            break;
                        }
                        case ColumnType.SHORT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                offset = buffer.addShort(offset, (short) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addShort(offset, (short) 0);
                            } else {
                                throw boundsError(entityValue, nEntity, ColumnType.SHORT);
                            }
                            break;
                        }
                        case ColumnType.BYTE: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                offset = buffer.addByte(offset, (byte) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addByte(offset, (byte) 0);
                            } else {
                                throw boundsError(entityValue, nEntity, ColumnType.BYTE);
                            }
                            break;
                        }
                        case ColumnType.TIMESTAMP:
                            long timestampValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(entity.getLongValue(), LineTcpParser.ENTITY_UNIT_NONE);
                            offset = buffer.addTimestamp(offset, timestampValue);
                            break;
                        case ColumnType.DATE:
                            offset = buffer.addDate(offset, entity.getLongValue());
                            break;
                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entity.getLongValue());
                            break;
                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, entity.getLongValue());
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    parser.hasNonAsciiChars(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError("integer", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_FLOAT: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entity.getFloatValue());
                            break;
                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, (float) entity.getFloatValue());
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError("float", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_STRING: {
                    final int colTypeMeta = localDetails.getColumnTypeMeta(columnWriterIndex);
                    final DirectUtf8Sequence entityValue = entity.getValue();
                    if (colTypeMeta == 0) { // not geohash
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.IPv4:
                                try {
                                    int value = Numbers.parseIPv4Nl(entityValue);
                                    offset = buffer.addInt(offset, value);
                                } catch (NumericException e) {
                                    throw castError("string", columnWriterIndex, colType, entity.getName());
                                }
                                break;
                            case ColumnType.STRING:
                                offset = buffer.addString(offset, entityValue, parser.hasNonAsciiChars());
                                break;
                            case ColumnType.CHAR:
                                if (entityValue.size() == 1 && entityValue.byteAt(0) > -1) {
                                    offset = buffer.addChar(offset, (char) entityValue.byteAt(0));
                                } else if (stringToCharCastAllowed) {
                                    int encodedResult = Utf8s.utf8CharDecode(entityValue.lo(), entityValue.hi());
                                    if (Numbers.decodeLowShort(encodedResult) > 0) {
                                        offset = buffer.addChar(offset, (char) Numbers.decodeHighShort(encodedResult));
                                    } else {
                                        throw castError("string", columnWriterIndex, colType, entity.getName());
                                    }
                                } else {
                                    throw castError("string", columnWriterIndex, colType, entity.getName());
                                }
                                break;
                            case ColumnType.SYMBOL:
                                offset = buffer.addSymbol(
                                        offset,
                                        entityValue,
                                        parser.hasNonAsciiChars(),
                                        localDetails.getSymbolLookup(columnWriterIndex)
                                );
                                break;
                            case ColumnType.UUID:
                                try {
                                    offset = buffer.addUuid(offset, entityValue);
                                } catch (NumericException e) {
                                    throw castError("string", columnWriterIndex, colType, entity.getName());
                                }
                                break;
                            default:
                                throw castError("string", columnWriterIndex, colType, entity.getName());
                        }
                    } else {
                        offset = buffer.addGeoHash(offset, entityValue, colTypeMeta);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_LONG256: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG256:
                            offset = buffer.addLong256(offset, entity.getValue(), parser.hasNonAsciiChars());
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    parser.hasNonAsciiChars(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError("long256", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                    byte entityValue = (byte) (entity.getBooleanValue() ? 1 : 0);
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.BOOLEAN:
                            offset = buffer.addBoolean(offset, entityValue);
                            break;
                        case ColumnType.BYTE:
                            offset = buffer.addByte(offset, entityValue);
                            break;
                        case ColumnType.SHORT:
                            offset = buffer.addShort(offset, entityValue);
                            break;
                        case ColumnType.INT:
                            offset = buffer.addInt(offset, entityValue);
                            break;
                        case ColumnType.LONG:
                            offset = buffer.addLong(offset, entityValue);
                            break;
                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, entityValue);
                            break;
                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entityValue);
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    parser.hasNonAsciiChars(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError("boolean", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.TIMESTAMP:
                            long timestampValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(entity.getLongValue(), entity.getUnit());
                            offset = buffer.addTimestamp(offset, timestampValue);
                            break;
                        case ColumnType.DATE:
                            long dateValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(entity.getLongValue(), entity.getUnit());
                            offset = buffer.addDate(offset, dateValue / 1000);
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    parser.hasNonAsciiChars(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError("timestamp", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        offset = buffer.addSymbol(
                                offset,
                                entity.getValue(),
                                parser.hasNonAsciiChars(),
                                localDetails.getSymbolLookup(columnWriterIndex)
                        );
                    } else {
                        throw castError("symbol", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case ENTITY_TYPE_NULL:
                    offset = buffer.addNull(offset);
                    break;
                default:
                    // unsupported types are ignored
                    break;
            }
        }
        buffer.addDesignatedTimestamp(buffer.getAddress() + Long.BYTES, timestamp);
        buffer.addNumOfColumns(buffer.getAddress() + 2 * Long.BYTES, entitiesWritten);
        writerWorkerId = tud.getWriterThreadId();
    }

    void createWriterReleaseEvent(TableUpdateDetails tableUpdateDetails, boolean commitOnWriterClose) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER;
        this.tableUpdateDetails = tableUpdateDetails;
        this.commitOnWriterClose = commitOnWriterClose;
    }

    private static class PrincipalOnlySecurityContext extends DenyAllSecurityContext {
        private CharSequence principal;

        @Override
        public CharSequence getPrincipal() {
            return principal;
        }

        public PrincipalOnlySecurityContext of(CharSequence principal) {
            this.principal = principal;
            return this;
        }
    }
}
