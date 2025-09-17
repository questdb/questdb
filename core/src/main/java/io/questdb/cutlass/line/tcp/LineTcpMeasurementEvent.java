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

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.LineProtocolException.*;
import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;

public class LineTcpMeasurementEvent implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementEvent.class);
    private final boolean autoCreateNewColumns;
    private final LineTcpEventBuffer buffer;
    private final DefaultColumnTypes defaultColumnTypes;
    private final int maxColumnNameLength;
    private final PrincipalOnlySecurityContext principalOnlySecurityContext = new PrincipalOnlySecurityContext();
    private final boolean stringToCharCastAllowed;
    private boolean commitOnWriterClose;
    private TableUpdateDetails tableUpdateDetails;
    private final byte timestampUnit;
    private int writerWorkerId;

    LineTcpMeasurementEvent(
            long bufLo,
            long bufSize,
            byte timestampUnit,
            DefaultColumnTypes defaultColumnTypes,
            boolean stringToCharCastAllowed,
            int maxColumnNameLength,
            boolean autoCreateNewColumns
    ) {
        this.maxColumnNameLength = maxColumnNameLength;
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.buffer = new LineTcpEventBuffer(bufLo, bufSize);
        this.defaultColumnTypes = defaultColumnTypes;
        this.timestampUnit = timestampUnit;
        this.stringToCharCastAllowed = stringToCharCastAllowed;
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

    private byte getOverloadTimestampUnit(byte unit) {
        switch (unit) {
            case CommonUtils.TIMESTAMP_UNIT_NANOS:
            case CommonUtils.TIMESTAMP_UNIT_MICROS:
            case CommonUtils.TIMESTAMP_UNIT_MILLIS:
                return unit;
        }
        return timestampUnit;
    }

    void append() throws CommitFailedException {
        TableWriter.Row row = null;
        try {
            final TableWriterAPI writer = tableUpdateDetails.getWriter();
            long address = buffer.getAddress();
            final long metadataVersion = buffer.readLong(address);
            address += Long.BYTES;
            if (metadataVersion > writer.getMetadataVersion()) {
                // I/O thread has a more recent version of the WAL table metadata than the writer.
                // Let the WAL writer commit, so that it refreshes its metadata copy.
                // TODO: this method is not used for WAL tables, check if the below commit still needed
                writer.commit();
            }
            long timestamp = buffer.readLong(address);
            address += Long.BYTES;
            if (timestamp == LineTcpParser.NULL_TIMESTAMP) {
                timestamp = tableUpdateDetails.getTimestampDriver().getTicks();
            }
            row = writer.newRow(timestamp);
            final int nEntities = buffer.readInt(address);
            address += Integer.BYTES;
            final long writerMetadataVersion = writer.getMetadataVersion();
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                int colIndex = buffer.readInt(address);
                address += Integer.BYTES;
                final byte entityType;
                if (colIndex > -1) {
                    entityType = buffer.readByte(address);
                    address += Byte.BYTES;
                    // Did the I/O thread have the latest structure version when it serialized the row?
                    if (metadataVersion < writerMetadataVersion) {
                        // Nope. For WAL tables, it could mean that the column is already dropped. Let's check it.
                        if (!writer.getMetadata().hasColumn(colIndex)) {
                            // The column was dropped, so we skip it.
                            address += buffer.columnValueLength(entityType, address);
                            continue;
                        }
                    }
                } else {
                    // Column is passed by name, it is possible that
                    // column is new and has to be added. It is also possible that column
                    // already exist but the publisher is a little out of date and does not yet
                    // have column index.

                    // Column name will be UTF16 encoded already
                    final CharSequence columnName = buffer.readUtf16Chars(address, -colIndex);
                    address += -colIndex * 2L;
                    // Column name is followed with the principal name.
                    int principalLen = buffer.readInt(address);
                    address += Integer.BYTES;
                    final CharSequence principal = buffer.readUtf16CharsB(address, principalLen);
                    address += principalLen * 2L;

                    entityType = buffer.readByte(address);
                    address += Byte.BYTES;
                    colIndex = writer.getMetadata().getColumnIndexQuiet(columnName);
                    if (colIndex < 0) {
                        // we have to cancel "active" row to avoid writer committing when
                        // column is added
                        row.cancel();
                        row = null;
                        final int colType = defaultColumnTypes.MAPPED_COLUMN_TYPES[entityType];
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
                        address = buffer.getAddressAfterHeader();
                        nEntity = -1;
                        row = writer.newRow(timestamp);
                        continue;
                    }
                }

                CharSequence cs;
                switch (entityType) {
                    case LineTcpParser.ENTITY_TYPE_TAG:
                        cs = buffer.readUtf16Chars(address);
                        row.putSym(colIndex, cs);
                        address += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_CACHED_TAG:
                        row.putSymIndex(colIndex, buffer.readInt(address));
                        address += Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_LONG:
                    case LineTcpParser.ENTITY_TYPE_GEOLONG:
                        row.putLong(colIndex, buffer.readLong(address));
                        address += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_INTEGER:
                    case LineTcpParser.ENTITY_TYPE_GEOINT:
                        row.putInt(colIndex, buffer.readInt(address));
                        address += Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_SHORT:
                    case LineTcpParser.ENTITY_TYPE_GEOSHORT:
                        row.putShort(colIndex, buffer.readShort(address));
                        address += Short.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_BYTE:
                    case LineTcpParser.ENTITY_TYPE_GEOBYTE:
                        row.putByte(colIndex, buffer.readByte(address));
                        address += Byte.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_DATE:
                        row.putDate(colIndex, buffer.readLong(address));
                        address += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_DOUBLE:
                        row.putDouble(colIndex, buffer.readDouble(address));
                        address += Double.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_FLOAT:
                        row.putFloat(colIndex, buffer.readFloat(address));
                        address += Float.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN:
                        row.putBool(colIndex, buffer.readByte(address) == 1);
                        address += Byte.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_STRING:
                        cs = buffer.readUtf16Chars(address);
                        row.putStr(colIndex, cs);
                        address += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_CHAR:
                        row.putChar(colIndex, buffer.readChar(address));
                        address += Character.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_LONG256:
                        cs = buffer.readUtf16Chars(address);
                        row.putLong256(colIndex, cs);
                        address += cs.length() * 2L + Integer.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP:
                        row.putTimestamp(colIndex, buffer.readLong(address));
                        address += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_UUID:
                        row.putLong128(colIndex, buffer.readLong(address), buffer.readLong(address + Long.BYTES));
                        address += Long.BYTES * 2;
                        break;
                    case LineTcpParser.ENTITY_TYPE_VARCHAR:
                        final boolean ascii = buffer.readByte(address++) == 0;
                        Utf8Sequence s = buffer.readVarchar(address, ascii);
                        row.putVarchar(colIndex, s);
                        address += Integer.BYTES + s.size();
                        break;
                    case LineTcpParser.ENTITY_TYPE_ARRAY:
                        ArrayView array = buffer.readArray(address);
                        row.putArray(colIndex, array);
                        address += buffer.columnValueLength(entityType, address);
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
        } catch (CommitFailedException | CairoError e) {
            throw e;
        } catch (Throwable th) {
            LOG.error()
                    .$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.getTableToken())
                    .$(", message=").$safe(th.getMessage())
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
            timestamp = tud.getTimestampDriver().from(timestamp, getOverloadTimestampUnit(parser.getTimestampUnit()));
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
            int columnWriterIndex = localDetails.getColumnWriterIndex(entity.getName());
            if (columnWriterIndex > -1) {
                // column index found, processing column by index
                if (columnWriterIndex == tud.getTimestampIndex()) {
                    timestamp = tud.getTimestampDriver().from(entity.getLongValue(), entity.getUnit());
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
                    colType = localDetails.getColumnType(localDetails.getColNameUtf8(), entity);
                } else if (!autoCreateNewColumns) {
                    throw newColumnsNotAllowed(colNameUtf16, tableUpdateDetails.getTableNameUtf16());
                } else {
                    throw invalidColNameError(colNameUtf16, tableUpdateDetails.getTableNameUtf16());
                }
            } else {
                // duplicate column, skip
                // we could set a boolean in the config if we want to throw exception instead
                continue;
            }

            entitiesWritten++;
            switch (entityType) {
                case LineTcpParser.ENTITY_TYPE_INTEGER: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG:
                            offset = buffer.addLong(offset, entity.getLongValue());
                            break;
                        case ColumnType.INT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Integer.MIN_VALUE && entityValue <= Integer.MAX_VALUE) {
                                offset = buffer.addInt(offset, (int) entityValue);
                            } else if (entityValue == Numbers.LONG_NULL) {
                                offset = buffer.addInt(offset, Numbers.INT_NULL);
                            } else {
                                throw boundsError(entityValue, ColumnType.INT, tud.getTableNameUtf16(), nEntity);
                            }
                            break;
                        }
                        case ColumnType.SHORT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                offset = buffer.addShort(offset, (short) entityValue);
                            } else if (entityValue == Numbers.LONG_NULL) {
                                offset = buffer.addShort(offset, (short) 0);
                            } else {
                                throw boundsError(entityValue, ColumnType.SHORT, tud.getTableNameUtf16(), nEntity);
                            }
                            break;
                        }
                        case ColumnType.BYTE: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                offset = buffer.addByte(offset, (byte) entityValue);
                            } else if (entityValue == Numbers.LONG_NULL) {
                                offset = buffer.addByte(offset, (byte) 0);
                            } else {
                                throw boundsError(entityValue, ColumnType.BYTE, tud.getTableNameUtf16(), nEntity);
                            }
                            break;
                        }
                        case ColumnType.TIMESTAMP:
                            long timestampValue = entity.getLongValue();
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
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "integer", colType, entity.getName());
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
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "float", colType, entity.getName());
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
                                    throw castError(tud.getTableNameUtf16(), "string", colType, entity.getName());
                                }
                                break;
                            case ColumnType.STRING:
                                offset = buffer.addString(offset, entityValue);
                                break;
                            case ColumnType.VARCHAR:
                                offset = buffer.addVarchar(offset, entityValue);
                                break;
                            case ColumnType.CHAR:
                                if (entityValue.size() == 1 && entityValue.byteAt(0) > -1) {
                                    offset = buffer.addChar(offset, (char) entityValue.byteAt(0));
                                } else if (stringToCharCastAllowed) {
                                    int encodedResult = Utf8s.utf8CharDecode(entityValue);
                                    if (Numbers.decodeLowShort(encodedResult) > 0) {
                                        offset = buffer.addChar(offset, (char) Numbers.decodeHighShort(encodedResult));
                                    } else {
                                        throw castError(tud.getTableNameUtf16(), "string", colType, entity.getName());
                                    }
                                } else {
                                    throw castError(tud.getTableNameUtf16(), "string", colType, entity.getName());
                                }
                                break;
                            case ColumnType.SYMBOL:
                                offset = buffer.addSymbol(
                                        offset,
                                        entityValue,
                                        localDetails.getSymbolLookup(columnWriterIndex)
                                );
                                break;
                            case ColumnType.UUID:
                                try {
                                    offset = buffer.addUuid(offset, entityValue);
                                } catch (NumericException e) {
                                    throw castError(tud.getTableNameUtf16(), "string", colType, entity.getName());
                                }
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "string", colType, entity.getName());
                        }
                    } else {
                        offset = buffer.addGeoHash(offset, entityValue, colTypeMeta);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_LONG256: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG256:
                            offset = buffer.addLong256(offset, entity.getValue());
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "long256", colType, entity.getName());
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
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "boolean", colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.TIMESTAMP:
                            long timestampValue = ColumnType.getTimestampDriver(colType).from(entity.getLongValue(), entity.getUnit());
                            offset = buffer.addTimestamp(offset, timestampValue);
                            break;
                        case ColumnType.DATE:
                            TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
                            long dateValue = driver.toDate(driver.from(entity.getLongValue(), entity.getUnit()));
                            offset = buffer.addDate(offset, dateValue);
                            break;
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "timestamp", colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TAG:
                case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                    switch (colType) {
                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(
                                    offset,
                                    entity.getValue(),
                                    localDetails.getSymbolLookup(columnWriterIndex)
                            );
                            break;
                        case ColumnType.VARCHAR:
                            offset = buffer.addVarchar(offset, entity.getValue());
                            break;
                        case ColumnType.STRING:
                            offset = buffer.addString(offset, entity.getValue());
                            break;
                        default:
                            throw castError(tud.getTableNameUtf16(), "symbol", colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_ARRAY:
                    BorrowedArray array = entity.getArray();
                    if (array.getType() != colType && !array.isNull()) {
                        throw castError(tud.getTableNameUtf16(), ColumnType.nameOf(array.getType()), colType, entity.getName());
                    }
                    offset = buffer.addArray(offset, array);
                    break;
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
