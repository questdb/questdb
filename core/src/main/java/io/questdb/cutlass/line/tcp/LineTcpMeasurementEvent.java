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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;

class LineTcpMeasurementEvent implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementEvent.class);
    private final MicrosecondClock clock;
    private final LineProtoTimestampAdapter timestampAdapter;
    private final long bufSize;
    private int writerWorkerId;
    private TableUpdateDetails tableUpdateDetails;
    private long bufLo;
    private int reshuffleSrcWorkerId;
    private int reshuffleTargetWorkerId;
    private volatile boolean reshuffleComplete;
    private boolean commitOnWriterClose;

    LineTcpMeasurementEvent(
            long bufLo,
            long bufSize,
            MicrosecondClock clock,
            LineProtoTimestampAdapter timestampAdapter
    ) {
        this.bufLo = bufLo;
        this.bufSize = bufSize;
        this.clock = clock;
        this.timestampAdapter = timestampAdapter;
    }

    @Override
    public void close() {
        // this is concurrent writer release
        tableUpdateDetails = Misc.free(tableUpdateDetails);
        bufLo = 0;
    }

    public int getReshuffleSrcWorkerId() {
        return reshuffleSrcWorkerId;
    }

    public int getReshuffleTargetWorkerId() {
        return reshuffleTargetWorkerId;
    }

    public TableUpdateDetails getTableUpdateDetails() {
        return tableUpdateDetails;
    }

    public int getWriterWorkerId() {
        return writerWorkerId;
    }

    public boolean isReshuffleComplete() {
        return reshuffleComplete;
    }

    public void setReshuffleComplete(boolean reshuffleComplete) {
        this.reshuffleComplete = reshuffleComplete;
    }

    public void releaseWriter() {
        tableUpdateDetails.releaseWriter(commitOnWriterClose);
    }

    void append(StringSink charSink, FloatingDirectCharSink floatingCharSink) {
        TableWriter.Row row = null;
        try {
            TableWriter writer = tableUpdateDetails.getWriter();
            long bufPos = bufLo;
            long timestamp = Unsafe.getUnsafe().getLong(bufPos);
            bufPos += Long.BYTES;
            if (timestamp == LineTcpParser.NULL_TIMESTAMP) {
                timestamp = clock.getTicks();
            }
            row = writer.newRow(timestamp);
            int nEntities = Unsafe.getUnsafe().getInt(bufPos);
            bufPos += Integer.BYTES;
            long firstEntityBufPos = bufPos;
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                int colIndex = Unsafe.getUnsafe().getInt(bufPos);
                bufPos += Integer.BYTES;
                byte entityType;
                if (colIndex > -1) {
                    entityType = Unsafe.getUnsafe().getByte(bufPos);
                    bufPos += Byte.BYTES;
                } else {
                    int colNameLen = -1 * colIndex;
                    long nameLo = bufPos; // UTF8 encoded
                    long nameHi = bufPos + colNameLen;
                    charSink.clear();
                    if (!Chars.utf8Decode(nameLo, nameHi, charSink)) {
                        throw CairoException.instance(0)
                                .put("invalid UTF8 in column name ");
                        // todo: dump hex of bad column name
                    }
                    bufPos = nameHi;
                    entityType = Unsafe.getUnsafe().getByte(bufPos);
                    bufPos += Byte.BYTES;
                    colIndex = writer.getMetadata().getColumnIndexQuiet(charSink);
                    if (colIndex < 0) {
                        // Cannot create a column with an open row, writer will commit when a column is created
                        row.cancel();
                        row = null;
                        int colType = DefaultColumnTypes.DEFAULT_COLUMN_TYPES[entityType];
                        if (TableUtils.isValidInfluxColumnName(charSink)) {
                            writer.addColumn(charSink, colType);
                        } else {
                            throw CairoException.instance(0)
                                    .put("invalid column name [table=").put(writer.getTableName())
                                    .put(", columnName=").put(charSink)
                                    .put(']');
                        }
                        // Reset to beginning of entities
                        bufPos = firstEntityBufPos;
                        nEntity = -1;
                        row = writer.newRow(timestamp);
                        continue;
                    }
                }

                switch (entityType) {
                    case LineTcpParser.ENTITY_TYPE_TAG: {
                        int len = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        long hi = bufPos + 2L * len;
                        floatingCharSink.asCharSequence(bufPos, hi);
                        int symIndex = writer.getSymbolIndex(colIndex, floatingCharSink);
                        row.putSymIndex(colIndex, symIndex);
                        bufPos = hi;
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_CACHED_TAG: {
                        int symIndex = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        row.putSymIndex(colIndex, symIndex);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_INTEGER: {
                        final int colType = ColumnType.tagOf(writer.getMetadata().getColumnType(colIndex));
                        long v = Unsafe.getUnsafe().getLong(bufPos);
                        bufPos += Long.BYTES;
                        switch (colType) {
                            case ColumnType.LONG:
                                row.putLong(colIndex, v);
                                break;

                            case ColumnType.INT:
                                if (v == Numbers.LONG_NaN) {
                                    v = Numbers.INT_NaN;
                                } else if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                                    throw CairoException.instance(0)
                                            .put("line protocol integer is out of int bounds [columnIndex=").put(colIndex)
                                            .put(", v=").put(v)
                                            .put(']');
                                }
                                row.putInt(colIndex, (int) v);
                                break;

                            case ColumnType.SHORT:
                                if (v == Numbers.LONG_NaN) {
                                    v = (short) 0;
                                } else if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                    throw CairoException.instance(0)
                                            .put("line protocol integer is out of short bounds [columnIndex=").put(colIndex)
                                            .put(", v=").put(v)
                                            .put(']');
                                }
                                row.putShort(colIndex, (short) v);
                                break;

                            case ColumnType.BYTE:
                                if (v == Numbers.LONG_NaN) {
                                    v = (byte) 0;
                                } else if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                                    throw CairoException.instance(0)
                                            .put("line protocol integer is out of byte bounds [columnIndex=").put(colIndex)
                                            .put(", v=").put(v)
                                            .put(']');
                                }
                                row.putByte(colIndex, (byte) v);
                                break;

                            case ColumnType.TIMESTAMP:
                                row.putTimestamp(colIndex, v);
                                break;

                            case ColumnType.DATE:
                                row.putDate(colIndex, v);
                                break;

                            default:
                                throw CairoException.instance(0)
                                        .put("cast error for line protocol integer [columnIndex=").put(colIndex)
                                        .put(", columnType=").put(ColumnType.nameOf(colType))
                                        .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_FLOAT: {
                        double v = Unsafe.getUnsafe().getDouble(bufPos);
                        bufPos += Double.BYTES;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.DOUBLE:
                                row.putDouble(colIndex, v);
                                break;

                            case ColumnType.FLOAT:
                                row.putFloat(colIndex, (float) v);
                                break;

                            default:
                                throw CairoException.instance(0)
                                        .put("cast error for line protocol float [columnIndex=").put(colIndex)
                                        .put(", columnType=").put(ColumnType.nameOf(colType))
                                        .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        byte b = Unsafe.getUnsafe().getByte(bufPos);
                        bufPos += Byte.BYTES;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.BOOLEAN:
                                row.putBool(colIndex, b == 1);
                                break;

                            case ColumnType.BYTE:
                                row.putByte(colIndex, b);
                                break;

                            case ColumnType.SHORT:
                                row.putShort(colIndex, b);
                                break;

                            case ColumnType.INT:
                                row.putInt(colIndex, b);
                                break;

                            case ColumnType.LONG:
                                row.putLong(colIndex, b);
                                break;

                            case ColumnType.FLOAT:
                                row.putFloat(colIndex, b);
                                break;

                            case ColumnType.DOUBLE:
                                row.putDouble(colIndex, b);
                                break;

                            default:
                                throw CairoException.instance(0)
                                        .put("cast error for line protocol boolean [columnIndex=").put(colIndex)
                                        .put(", columnType=").put(ColumnType.nameOf(colType))
                                        .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_STRING: {
                        int len = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        long hi = bufPos + 2L * len;
                        floatingCharSink.asCharSequence(bufPos, hi);
                        bufPos = hi;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        if (ColumnType.isString(colType)) {
                            row.putStr(colIndex, floatingCharSink);
                        } else if (ColumnType.isChar(colType)) {
                            row.putChar(colIndex, floatingCharSink.charAt(0));
                        } else {
                            throw CairoException.instance(0)
                                    .put("cast error for line protocol string [columnIndex=").put(colIndex)
                                    .put(", columnType=").put(ColumnType.nameOf(colType))
                                    .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                        int len = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        long hi = bufPos + 2L * len;
                        floatingCharSink.asCharSequence(bufPos, hi);
                        bufPos = hi;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        if (ColumnType.isSymbol(colType)) {
                            row.putSym(colIndex, floatingCharSink);
                        } else {
                            throw CairoException.instance(0)
                                    .put("cast error for line protocol symbol [columnIndex=").put(colIndex)
                                    .put(", columnType=").put(ColumnType.nameOf(colType))
                                    .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        int len = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        long hi = bufPos + 2L * len;
                        floatingCharSink.asCharSequence(bufPos, hi);
                        bufPos = hi;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        if (ColumnType.isLong256(colType)) {
                            row.putLong256(colIndex, floatingCharSink);
                        } else {
                            throw CairoException.instance(0)
                                    .put("cast error for line protocol long256 [columnIndex=").put(colIndex)
                                    .put(", columnType=").put(ColumnType.nameOf(colType))
                                    .put(']');
                        }
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_GEOLONG: {
                        long geoHash = Unsafe.getUnsafe().getLong(bufPos);
                        bufPos += Long.BYTES;
                        row.putLong(colIndex, geoHash);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_GEOINT: {
                        int geoHash = Unsafe.getUnsafe().getInt(bufPos);
                        bufPos += Integer.BYTES;
                        row.putInt(colIndex, geoHash);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_GEOSHORT: {
                        short geohash = Unsafe.getUnsafe().getShort(bufPos);
                        bufPos += Short.BYTES;
                        row.putShort(colIndex, geohash);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_GEOBYTE: {
                        byte geohash = Unsafe.getUnsafe().getByte(bufPos);
                        bufPos += Byte.BYTES;
                        row.putByte(colIndex, geohash);
                        break;
                    }

                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        long ts = Unsafe.getUnsafe().getLong(bufPos);
                        bufPos += Long.BYTES;
                        final int colType = writer.getMetadata().getColumnType(colIndex);
                        if (ColumnType.isTimestamp(colType)) {
                            row.putTimestamp(colIndex, ts);
                        } else {
                            throw CairoException.instance(0)
                                    .put("cast error for line protocol timestamp [columnIndex=").put(colIndex)
                                    .put(", columnType=").put(ColumnType.nameOf(colType))
                                    .put(']');
                        }
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
            tableUpdateDetails.handleRowAppended();
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.getTableNameUtf16())
                    .$(", ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
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
        final BoolList processedCols = localDetails.getProcessedCols();
        final LowerCaseCharSequenceHashSet addedCols = localDetails.getAddedCols();
        processedCols.setAll(localDetails.getColumnCount(), false);
        addedCols.clear();
        this.tableUpdateDetails = tableUpdateDetails;
        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp);
            processedCols.setQuick(tableUpdateDetails.getTimestampIndex(), true);
        }
        long bufPos = bufLo;
        long bufMax = bufLo + bufSize;
        long timestampBufPos = bufPos;
        //timestamp and entitiesWritten are saved to timestampBufPos after saving all fields
        bufPos += Long.BYTES;
        int entitiesWritten = 0;
        bufPos += Integer.BYTES;
        for (int nEntity = 0, n = parser.getEntityCount(); nEntity < n; nEntity++) {
            if (bufPos + Long.BYTES < bufMax) {
                LineTcpParser.ProtoEntity entity = parser.getEntity(nEntity);
                int colIndex = localDetails.getColumnIndex(entity.getName());
                if (colIndex < 0) {
                    final DirectByteCharSequence colName = entity.getName();
                    if (!addedCols.add(colName)) {
                        continue;
                    }
                    int colNameLen = colName.length();
                    Unsafe.getUnsafe().putInt(bufPos, -1 * colNameLen);
                    bufPos += Integer.BYTES;
                    if (bufPos + colNameLen < bufMax) {
                        // Memcpy the buffer with the column name to the message
                        // so that writing thread will create the column
                        // Note that writing thread will be responsible to convert it from utf8
                        // to utf16. This should happen rarely
                        Vect.memcpy(bufPos, colName.getLo(), colNameLen);
                    } else {
                        throw CairoException.instance(0).put("queue buffer overflow");
                    }
                    bufPos += colNameLen;
                } else {
                    if (colIndex == tableUpdateDetails.getTimestampIndex()) {
                        timestamp = timestampAdapter.getMicros(entity.getLongValue());
                        continue;
                    }
                    if (!processedCols.extendAndReplace(colIndex, true)) {
                        Unsafe.getUnsafe().putInt(bufPos, colIndex);
                        bufPos += Integer.BYTES;
                    } else {
                        continue;
                    }
                }
                entitiesWritten++;
                switch (entity.getType()) {
                    case LineTcpParser.ENTITY_TYPE_TAG: {
                        long tmpBufPos = bufPos;
                        int l = entity.getValue().length();
                        bufPos += Integer.BYTES + Byte.BYTES;
                        long estimatedHi = bufPos + 2L * l;
                        if (estimatedHi < bufMax) {
                            floatingCharSink.of(bufPos, bufPos + 2L * l);
                            int symIndex;
                            // value is UTF8 encoded potentially
                            CharSequence columnValue = entity.getValue();
                            if (parser.hasNonAsciiChars()) {
                                if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                                    throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                                }
                                columnValue = floatingCharSink;
                            }

                            symIndex = tableUpdateDetails.getSymbolIndex(localDetails, colIndex, columnValue);
                            if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
                                // We know the symbol int value
                                // Encode the int
                                bufPos = tmpBufPos;
                                Unsafe.getUnsafe().putByte(bufPos, LineTcpParser.ENTITY_TYPE_CACHED_TAG);
                                bufPos += Byte.BYTES;
                                Unsafe.getUnsafe().putInt(bufPos, symIndex);
                                bufPos += Integer.BYTES;
                            } else {
                                // Symbol value cannot be resolved at this point
                                // Encode whole string value into the message
                                Unsafe.getUnsafe().putByte(tmpBufPos, entity.getType());
                                tmpBufPos += Byte.BYTES;
                                if (!parser.hasNonAsciiChars()) {
                                    // if it is non-ascii, then value already copied to the buffer
                                    floatingCharSink.put(entity.getValue());
                                }
                                l = floatingCharSink.length();
                                Unsafe.getUnsafe().putInt(tmpBufPos, l);
                                bufPos = bufPos + 2L * l;
                            }
                        } else {
                            throw CairoException.instance(0).put("queue buffer overflow");
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_INTEGER:
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putLong(bufPos, entity.getLongValue());
                        bufPos += Long.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_FLOAT:
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putDouble(bufPos, entity.getFloatValue());
                        bufPos += Double.BYTES;
                        break;
                    case LineTcpParser.ENTITY_TYPE_STRING:
                    case LineTcpParser.ENTITY_TYPE_SYMBOL:
                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        final int colTypeMeta = localDetails.getColumnTypeMeta(colIndex);
                        if (colTypeMeta == 0) { // not a geohash
                            Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                            bufPos += Byte.BYTES + Integer.BYTES;
                            floatingCharSink.of(bufPos, bufPos + 2L * entity.getValue().length());
                            if (parser.hasNonAsciiChars()) {
                                if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                                    throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                                }
                            } else {
                                floatingCharSink.put(entity.getValue());
                            }
                            int l = floatingCharSink.length();
                            Unsafe.getUnsafe().putInt(bufPos - Integer.BYTES, l);
                            bufPos += floatingCharSink.length() * 2L;

                        } else {
                            long geohash;
                            try {
                                geohash = GeoHashes.fromStringTruncatingNl(
                                        entity.getValue().getLo(),
                                        entity.getValue().getHi(),
                                        Numbers.decodeLowShort(colTypeMeta)
                                );
                            } catch (NumericException e) {
                                geohash = GeoHashes.NULL;
                            }
                            switch (Numbers.decodeHighShort(colTypeMeta)) {
                                default:
                                    Unsafe.getUnsafe().putByte(bufPos, LineTcpParser.ENTITY_TYPE_GEOLONG);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putLong(bufPos, geohash);
                                    bufPos += Long.BYTES;
                                    break;
                                case ColumnType.GEOINT:
                                    Unsafe.getUnsafe().putByte(bufPos, LineTcpParser.ENTITY_TYPE_GEOINT);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putInt(bufPos, (int) geohash);
                                    bufPos += Integer.BYTES;
                                    break;
                                case ColumnType.GEOSHORT:
                                    Unsafe.getUnsafe().putByte(bufPos, LineTcpParser.ENTITY_TYPE_GEOSHORT);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putShort(bufPos, (short) geohash);
                                    bufPos += Short.BYTES;
                                    break;
                                case ColumnType.GEOBYTE:
                                    Unsafe.getUnsafe().putByte(bufPos, LineTcpParser.ENTITY_TYPE_GEOBYTE);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putByte(bufPos, (byte) geohash);
                                    bufPos += Byte.BYTES;
                                    break;
                            }
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putByte(bufPos, (byte) (entity.getBooleanValue() ? 1 : 0));
                        bufPos += Byte.BYTES;
                        break;
                    }
                    case ENTITY_TYPE_NULL: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putLong(bufPos, entity.getLongValue());
                        bufPos += Long.BYTES;
                        break;
                    }
                    default:
                        // unsupported types are ignored
                        break;
                }
            } else {
                throw CairoException.instance(0).put("queue buffer overflow");
            }
        }
        Unsafe.getUnsafe().putLong(timestampBufPos, timestamp);
        Unsafe.getUnsafe().putInt(timestampBufPos + Long.BYTES, entitiesWritten);
        writerWorkerId = tableUpdateDetails.getWriterThreadId();
    }

    void createReshuffleEvent(int fromThreadId, int toThreadId, TableUpdateDetails tableUpdateDetails) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_RESHUFFLE;
        reshuffleSrcWorkerId = fromThreadId;
        reshuffleTargetWorkerId = toThreadId;
        this.tableUpdateDetails = tableUpdateDetails;
        reshuffleComplete = false;
    }

    void createWriterReleaseEvent(TableUpdateDetails tableUpdateDetails, boolean commitOnWriterClose) {
        writerWorkerId = LineTcpMeasurementEventType.ALL_WRITERS_RELEASE_WRITER;
        this.tableUpdateDetails = tableUpdateDetails;
        this.commitOnWriterClose = commitOnWriterClose;
    }
}
