/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;

class LineTcpMeasurementEvent implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementEvent.class);
    private final MicrosecondClock clock;
    private final LineProtoTimestampAdapter timestampAdapter;
    private final LineTcpEventBuffer buffer;
    private final boolean stringToCharCastAllowed;
    private final boolean symbolAsFieldSupported;
    private int writerWorkerId;
    private TableUpdateDetails tableUpdateDetails;
    private boolean commitOnWriterClose;

    LineTcpMeasurementEvent(
            long bufLo,
            long bufSize,
            MicrosecondClock clock,
            LineProtoTimestampAdapter timestampAdapter,
            boolean stringToCharCastAllowed,
            boolean symbolAsFieldSupported
    ) {
        this.buffer = new LineTcpEventBuffer(bufLo, bufSize);
        this.clock = clock;
        this.timestampAdapter = timestampAdapter;
        this.stringToCharCastAllowed = stringToCharCastAllowed;
        this.symbolAsFieldSupported = symbolAsFieldSupported;
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

    void append() {
        TableWriter.Row row = null;
        try {
            TableWriter writer = tableUpdateDetails.getWriter();
            long offset = buffer.getAddress();
            long timestamp = buffer.readLong(offset);
            offset += Long.BYTES;
            if (timestamp == LineTcpParser.NULL_TIMESTAMP) {
                timestamp = clock.getTicks();
            }
            row = writer.newRow(timestamp);
            int nEntities = buffer.readInt(offset);
            offset += Integer.BYTES;
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                int colIndex = buffer.readInt(offset);
                offset += Integer.BYTES;
                byte entityType;
                if (colIndex > -1) {
                    entityType = buffer.readByte(offset);
                    offset += Byte.BYTES;
                } else {
                    // Column is passed by name, it is possible that
                    // column is new and has to be added. It is also possible that column
                    // already exist but the publisher is a little out of date and does not yet
                    // have column index.

                    // Column name will be UTF16 encoded already
                    final CharSequence columnName = buffer.readUtf16Chars(offset, -colIndex);
                    offset += -colIndex * 2L;

                    entityType = buffer.readByte(offset);
                    offset += Byte.BYTES;
                    colIndex = writer.getMetadata().getColumnIndexQuiet(columnName);
                    if (colIndex < 0) {
                        // we have to cancel "active" row to avoid writer committing when
                        // column is added
                        row.cancel();
                        row = null;
                        final int colType = DefaultColumnTypes.DEFAULT_COLUMN_TYPES[entityType];
                        writer.addColumn(columnName, colType);

                        // Seek to beginning of entities
                        offset = Long.BYTES + Integer.BYTES + buffer.getAddress();
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
                    case ENTITY_TYPE_NULL:
                        // ignored, default nulls is used
                        break;
                    default:
                        throw new UnsupportedOperationException("entityType " + entityType + " is not implemented!");
                }
            }
            row.append();
            tableUpdateDetails.commitIfMaxUncommittedRowsCountReached();
        } catch (Throwable th) {
            LOG.error()
                    .$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.getTableNameUtf16())
                    .$(", message=").$(th.getMessage())
                    .I$();
            if (row != null) {
                row.cancel();
            }
        }
    }

    private CairoException boundsError(long entityValue, int colIndex, int colType) {
        return CairoException.instance(0)
                .put("line protocol integer is out of ").put(ColumnType.nameOf(colType))
                .put(" bounds [columnIndex=").put(colIndex)
                .put(", value=").put(entityValue)
                .put(']');
    }

    private CairoException castError(String ilpType, int colIndex, int colType) {
        return CairoException.instance(0)
                .put("cast error for line protocol ").put(ilpType)
                .put(" [columnIndex=").put(colIndex)
                .put(", columnType=").put(ColumnType.nameOf(colType))
                .put(']');
    }

    void createMeasurementEvent(
            TableUpdateDetails tableUpdateDetails,
            LineTcpParser parser,
            int workerId
    ) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_INCOMPLETE_EVENT;
        final TableUpdateDetails.ThreadLocalDetails localDetails = tableUpdateDetails.getThreadLocalDetails(workerId);
        localDetails.resetProcessedColumnsTracking();
        this.tableUpdateDetails = tableUpdateDetails;
        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp);
        }
        // timestamp and entitiesWritten are saved to timestampBufPos after saving all fields
        // because their values are worked out while the columns are processed
        long offset = Long.BYTES + Integer.BYTES + buffer.getAddress();
//        buffer.seekToEntities();
        int entitiesWritten = 0;
        for (int nEntity = 0, n = parser.getEntityCount(); nEntity < n; nEntity++) {
            LineTcpParser.ProtoEntity entity = parser.getEntity(nEntity);
            byte entityType = entity.getType();
            int colType;
            int colIndex = localDetails.getColumnIndex(entity.getName(), parser.hasNonAsciiChars());
            if (colIndex > -1) {
                // column index found, processing column by index
                if (colIndex == tableUpdateDetails.getTimestampIndex()) {
                    timestamp = timestampAdapter.getMicros(entity.getLongValue());
                    continue;
                }

                offset = buffer.addColumnIndex(offset, colIndex);
                colType = localDetails.getColumnType(colIndex);
            } else if (colIndex == COLUMN_NOT_FOUND) {
                // send column by name
                CharSequence colName = localDetails.getColName();
                if (TableUtils.isValidColumnName(colName)) {
                    offset = buffer.addColumnName(offset, colName);
                    colType = DefaultColumnTypes.DEFAULT_COLUMN_TYPES[entityType];
                } else {
                    throw invalidColNameError(colName);
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
                        offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                    } else {
                        throw castError("tag", colIndex, colType);
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
                                throw boundsError(entityValue, colIndex, ColumnType.INT);
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
                                throw boundsError(entityValue, colIndex, ColumnType.SHORT);
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
                                throw boundsError(entityValue, colIndex, ColumnType.BYTE);
                            }
                            break;
                        }
                        case ColumnType.TIMESTAMP:
                            offset = buffer.addTimestamp(offset, entity.getLongValue());
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

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                            } else {
                                throw castError("integer", colIndex, colType);
                            }
                            break;
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

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                            } else {
                                throw castError("float", colIndex, colType);
                            }
                            break;
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_STRING: {
                    final int colTypeMeta = localDetails.getColumnTypeMeta(colIndex);
                    final DirectByteCharSequence entityValue = entity.getValue();
                    if (colTypeMeta == 0) { // not geohash
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.STRING:
                                offset = buffer.addString(offset, entityValue, parser.hasNonAsciiChars());
                                break;

                            case ColumnType.CHAR:
                                if (stringToCharCastAllowed || entityValue.length() == 1) {
                                    offset = buffer.addChar(offset, entityValue.charAt(0));
                                } else {
                                    throw castError("string", colIndex, colType);
                                }
                                break;

                            default:
                                if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                    offset = buffer.addSymbol(offset, entityValue, parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                                } else {
                                    throw castError("string", colIndex, colType);
                                }
                        }
                    } else {
                        offset = buffer.addGeoHash(offset, entityValue, colTypeMeta);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_LONG256: {
                    if (ColumnType.tagOf(colType) == ColumnType.LONG256) {
                        offset = buffer.addLong256(offset, entity.getValue(), parser.hasNonAsciiChars());
                    } else if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                        // todo: was someone doing this?
                        offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                    } else {
                        throw castError("long256", colIndex, colType);
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

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                            } else {
                                throw castError("boolean", colIndex, colType);
                            }
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                    if (ColumnType.tagOf(colType) == ColumnType.TIMESTAMP) {
                        offset = buffer.addTimestamp(offset, entity.getLongValue());
                    } else if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                        // todo: this makes no sense
                        offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(colIndex));
                    } else {
                        throw castError("timestamp", colIndex, colType);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        offset = buffer.addSymbol(
                                offset,
                                entity.getValue(),
                                parser.hasNonAsciiChars(),
                                localDetails.getSymbolLookup(colIndex)
                        );
                    } else {
                        throw castError("symbol", colIndex, colType);
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
        buffer.addDesignatedTimestamp(buffer.getAddress(), timestamp);
        buffer.addNumOfColumns(buffer.getAddress() + Long.BYTES, entitiesWritten);
        writerWorkerId = tableUpdateDetails.getWriterThreadId();
    }

    void createWriterReleaseEvent(TableUpdateDetails tableUpdateDetails, boolean commitOnWriterClose) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER;
        this.tableUpdateDetails = tableUpdateDetails;
        this.commitOnWriterClose = commitOnWriterClose;
    }

    private CairoException invalidColNameError(CharSequence colName) {
        return CairoException.instance(0)
                .put("invalid column name [table=").put(tableUpdateDetails.getTableNameUtf16())
                .put(", columnName=").put(colName)
                .put(']');
    }
}
