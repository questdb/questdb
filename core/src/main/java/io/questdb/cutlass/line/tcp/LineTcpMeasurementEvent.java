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

import io.questdb.cairo.*;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.FloatingDirectCharSink;

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

    void append(FloatingDirectCharSink floatingCharSink) {
        TableWriter.Row row = null;
        try {
            TableWriter writer = tableUpdateDetails.getWriter();
            buffer.reset();
            long timestamp = buffer.readLong();
            if (timestamp == LineTcpParser.NULL_TIMESTAMP) {
                timestamp = clock.getTicks();
            }
            row = writer.newRow(timestamp);
            int nEntities = buffer.readInt();
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                int colIndex = buffer.readInt();
                byte entityType;
                if (colIndex > -1) {
                    entityType = buffer.readByte();
                } else {
                    // Column is passed by name, it is possible that
                    // column is new and has to be added. It is also possible that column
                    // already exist but the publisher is a little out of date and does not yet
                    // have column index.

                    // Column name will be UTF16 encoded already
                    buffer.readUtf16Chars(floatingCharSink, -colIndex);
                    entityType = buffer.readByte();
                    colIndex = writer.getMetadata().getColumnIndexQuiet(floatingCharSink);
                    if (colIndex < 0) {
                        // we have to cancel "active" row to avoid writer committing when
                        // column is added
                        row.cancel();
                        row = null;
                        final int colType = DefaultColumnTypes.DEFAULT_COLUMN_TYPES[entityType];
                        writer.addColumn(floatingCharSink, colType);

                        // Seek to beginning of entities
                        buffer.seekToEntities();
                        nEntity = -1;
                        row = writer.newRow(timestamp);
                        continue;
                    }
                }

                switch (entityType) {
                    case LineTcpParser.ENTITY_TYPE_TAG: {
                        buffer.readUtf16Chars(floatingCharSink);
                        row.putSym(colIndex, floatingCharSink);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_CACHED_TAG: {
                        int symIndex = buffer.readInt();
                        row.putSymIndex(colIndex, symIndex);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_LONG:
                    case LineTcpParser.ENTITY_TYPE_GEOLONG: {
                        long v = buffer.readLong();
                        row.putLong(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_INTEGER:
                    case LineTcpParser.ENTITY_TYPE_GEOINT: {
                        int v = buffer.readInt();
                        row.putInt(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_SHORT:
                    case LineTcpParser.ENTITY_TYPE_GEOSHORT: {
                        short v = buffer.readShort();
                        row.putShort(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_BYTE:
                    case LineTcpParser.ENTITY_TYPE_GEOBYTE: {
                        byte v = buffer.readByte();
                        row.putByte(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_DATE: {
                        long v = buffer.readLong();
                        row.putDate(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_DOUBLE: {
                        double v = buffer.readDouble();
                        row.putDouble(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_FLOAT: {
                        float v = buffer.readFloat();
                        row.putFloat(colIndex, v);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        byte b = buffer.readByte();
                        row.putBool(colIndex, b == 1);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_STRING: {
                        buffer.readUtf16Chars(floatingCharSink);
                        row.putStr(colIndex, floatingCharSink);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_CHAR: {
                        char c = buffer.readChar();
                        row.putChar(colIndex, c);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        buffer.readUtf16Chars(floatingCharSink);
                        row.putLong256(colIndex, floatingCharSink);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        long ts = buffer.readLong();
                        row.putTimestamp(colIndex, ts);
                        break;
                    }

                    case ENTITY_TYPE_NULL: {
                        // ignored, default nulls is used
                        break;
                    }

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

    void createMeasurementEvent(
            TableUpdateDetails tableUpdateDetails,
            LineTcpParser parser,
            FloatingDirectCharSink floatingCharSink,
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
        buffer.seekToEntities();
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

                buffer.addColumnIndex(colIndex);
                colType = localDetails.getColumnType(colIndex);
            } else if (colIndex == COLUMN_NOT_FOUND) {
                // send column by name
                CharSequence colName = localDetails.getColName();
                if (TableUtils.isValidColumnName(colName)) {
                    buffer.addColumnName(colName);
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
                        buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                    } else {
                        throw castError("tag", colIndex, colType);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_INTEGER: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG:
                            buffer.addLong(entity.getLongValue());
                            break;

                        case ColumnType.INT: {
                            long entityValue = entity.getLongValue();
                            if (entityValue == Numbers.LONG_NaN) {
                                entityValue = Numbers.INT_NaN;
                            } else if (entityValue < Integer.MIN_VALUE || entityValue > Integer.MAX_VALUE) {
                                throw boundsError(entityValue, colIndex, ColumnType.INT);
                            }
                            buffer.addInt((int) entityValue);
                            break;
                        }
                        case ColumnType.SHORT: {
                            long entityValue = entity.getLongValue();
                            if (entityValue == Numbers.LONG_NaN) {
                                entityValue = (short) 0;
                            } else if (entityValue < Short.MIN_VALUE || entityValue > Short.MAX_VALUE) {
                                throw boundsError(entityValue, colIndex, ColumnType.SHORT);
                            }
                            buffer.addShort((short) entityValue);
                            break;
                        }
                        case ColumnType.BYTE: {
                            long entityValue = entity.getLongValue();
                            if (entityValue == Numbers.LONG_NaN) {
                                entityValue = (byte) 0;
                            } else if (entityValue < Byte.MIN_VALUE || entityValue > Byte.MAX_VALUE) {
                                throw boundsError(entityValue, colIndex, ColumnType.BYTE);
                            }
                            buffer.addByte((byte) entityValue);
                            break;
                        }
                        case ColumnType.TIMESTAMP:
                            buffer.addTimestamp(entity.getLongValue());
                            break;

                        case ColumnType.DATE:
                            buffer.addDate(entity.getLongValue());
                            break;

                        case ColumnType.DOUBLE:
                            buffer.addDouble(entity.getLongValue());
                            break;

                        case ColumnType.FLOAT:
                            buffer.addFloat(entity.getLongValue());
                            break;

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                            } else {
                                throw castError("integer", colIndex, colType);
                            }
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_FLOAT: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.DOUBLE:
                            buffer.addDouble(entity.getFloatValue());
                            break;

                        case ColumnType.FLOAT:
                            buffer.addFloat((float) entity.getFloatValue());
                            break;

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                            } else {
                                throw castError("float", colIndex, colType);
                            }
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_STRING: {
                    final int colTypeMeta = localDetails.getColumnTypeMeta(colIndex);
                    if (colTypeMeta != 0) { // geohash
                        buffer.addGeoHash(entity.getValue(), colTypeMeta);
                    } else {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.STRING:
                                buffer.addString(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars());
                                break;

                            case ColumnType.CHAR:
                                CharSequence entityValue = entity.getValue();
                                if (stringToCharCastAllowed || entityValue.length() == 1) {
                                    buffer.addChar(entityValue.charAt(0));
                                } else {
                                    throw castError("string", colIndex, colType);
                                }
                                break;

                            default:
                                if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                    buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                                } else {
                                    throw castError("string", colIndex, colType);
                                }
                        }
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_LONG256: {
                    if (ColumnType.tagOf(colType) == ColumnType.LONG256) {
                        buffer.addLong256(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars());
                    } else if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                        buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                    } else {
                        throw castError("long256", colIndex, colType);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                    byte entityValue = (byte) (entity.getBooleanValue() ? 1 : 0);
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.BOOLEAN:
                            buffer.addBoolean(entityValue);
                            break;

                        case ColumnType.BYTE:
                            buffer.addByte(entityValue);
                            break;

                        case ColumnType.SHORT:
                            buffer.addShort(entityValue);
                            break;

                        case ColumnType.INT:
                            buffer.addInt(entityValue);
                            break;

                        case ColumnType.LONG:
                            buffer.addLong(entityValue);
                            break;

                        case ColumnType.FLOAT:
                            buffer.addFloat(entityValue);
                            break;

                        case ColumnType.DOUBLE:
                            buffer.addDouble(entityValue);
                            break;

                        default:
                            if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                                buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                            } else {
                                throw castError("boolean", colIndex, colType);
                            }
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                    if (ColumnType.tagOf(colType) == ColumnType.TIMESTAMP) {
                        buffer.addTimestamp(entity.getLongValue());
                    } else if (symbolAsFieldSupported && colType == ColumnType.SYMBOL) {
                        buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                    } else {
                        throw castError("timestamp", colIndex, colType);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                    final int colTypeMeta = localDetails.getColumnTypeMeta(colIndex);
                    if (colTypeMeta != 0) { // geohash
                        buffer.addGeoHash(entity.getValue(), colTypeMeta);
                    } else if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        buffer.addSymbol(entity.getValue(), floatingCharSink, parser.hasNonAsciiChars(), localDetails, colIndex);
                    } else {
                        throw castError("symbol", colIndex, colType);
                    }
                    break;
                }
                case ENTITY_TYPE_NULL:
                    buffer.addNull();
                    break;
                default:
                    // unsupported types are ignored
                    break;
            }
        }
        buffer.reset();
        buffer.addDesignatedTimestamp(timestamp);
        buffer.addNumOfColumns(entitiesWritten);
        writerWorkerId = tableUpdateDetails.getWriterThreadId();
    }

    void createWriterReleaseEvent(TableUpdateDetails tableUpdateDetails, boolean commitOnWriterClose) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER;
        this.tableUpdateDetails = tableUpdateDetails;
        this.commitOnWriterClose = commitOnWriterClose;
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

    private CairoException invalidColNameError(CharSequence colName) {
        return CairoException.instance(0)
                .put("invalid column name [table=").put(tableUpdateDetails.getTableNameUtf16())
                .put(", columnName=").put(colName)
                .put(']');
    }
}
