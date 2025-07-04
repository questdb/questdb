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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.line.tcp.LineProtocolException.*;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.DUPLICATED_COLUMN;

public class LineWalAppender {
    private static final Log LOG = LogFactory.getLog(LineWalAppender.class);
    private final boolean autoCreateNewColumns;
    private final Long256Impl long256;
    private final int maxFileNameLength;
    private final boolean stringToCharCastAllowed;
    private byte timestampUnit;

    public LineWalAppender(
            boolean autoCreateNewColumns,
            boolean stringToCharCastAllowed,
            byte timestampUnit,
            int maxFileNameLength
    ) {
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.stringToCharCastAllowed = stringToCharCastAllowed;
        this.maxFileNameLength = maxFileNameLength;
        this.timestampUnit = timestampUnit;
        this.long256 = new Long256Impl();
    }

    public void appendToWal(
            SecurityContext securityContext,
            LineTcpParser parser,
            TableUpdateDetails tud
    ) throws CommitFailedException {
        while (!tud.isDropped()) {
            try {
                appendToWal0(securityContext, parser, tud);
                break;
            } catch (MetadataChangedException e) {
                // do another retry, metadata has changed while processing the line
                // and all the resolved column indexes have been invalidated
            }
        }
    }

    public void setTimestampAdapter(byte precision) {
        this.timestampUnit = precision;
    }

    private void appendToWal0(
            SecurityContext securityContext,
            LineTcpParser parser,
            TableUpdateDetails tud
    ) throws CommitFailedException, MetadataChangedException {

        // pass 1: create all columns that do not exist
        final TableUpdateDetails.ThreadLocalDetails ld = tud.getThreadLocalDetails(0); // IO thread id is irrelevant
        ld.resetStateIfNecessary();
        ld.clearColumnTypes();

        final TableWriterAPI writer = tud.getWriter();
        assert writer.supportsMultipleWriters();
        TableRecordMetadata metadata = writer.getMetadata();

        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            if (timestamp < 0) {
                throw LineProtocolException.designatedTimestampMustBePositive(tud.getTableNameUtf16(), timestamp);
            }
            timestamp = tud.getTimestampDriver().from(timestamp, getOverloadTimestampUnit(parser.getTimestampUnit()));
            if (timestamp > CommonUtils.MAX_TIMESTAMP) {
                throw LineProtocolException.designatedTimestampValueOverflow(tud.getTableNameUtf16(), timestamp);
            }
        } else {
            timestamp = tud.getTimestampDriver().getTicks();
        }

        final int entCount = parser.getEntityCount();
        for (int i = 0; i < entCount; i++) {
            final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
            int columnWriterIndex = ld.getColumnWriterIndex(ent.getName(), metadata);

            switch (columnWriterIndex) {
                default:
                    final int columnType = metadata.getColumnType(columnWriterIndex);
                    if (columnType > -1) {
                        if (columnWriterIndex == tud.getTimestampIndex()) {
                            timestamp = tud.getTimestampDriver().from(ent.getLongValue(), ent.getUnit());
                            ld.addColumnType(DUPLICATED_COLUMN, ColumnType.UNDEFINED);
                        } else {
                            ld.addColumnType(columnWriterIndex, metadata.getColumnType(columnWriterIndex));
                        }
                        break;
                    } else {
                        // column has been deleted from the metadata, but it is in our utf8 cache
                        ld.removeFromCaches(ent.getName());
                        // act as if we did not find this column and fall through
                    }
                case COLUMN_NOT_FOUND:
                    final String columnNameUtf16 = ld.getColNameUtf16();
                    if (autoCreateNewColumns && TableUtils.isValidColumnName(columnNameUtf16, maxFileNameLength)) {
                        columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                        if (columnWriterIndex < 0) {
                            securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                            try {
                                int newColumnType = ld.getColumnType(ld.getColNameUtf8(), ent);
                                writer.addColumn(columnNameUtf16, newColumnType, securityContext);
                                columnWriterIndex = metadata.getWriterIndex(metadata.getColumnIndexQuiet(columnNameUtf16));
                                // Add the column to metadata cache too
                                ld.addColumn(columnNameUtf16, columnWriterIndex, newColumnType);
                            } catch (CairoException e) {
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                                if (columnWriterIndex < 0) {
                                    // the column is still not there, something must be wrong
                                    throw e;
                                }
                                // all good, someone added the column concurrently
                            }
                        }
                        if (ld.getMetadataVersion() != writer.getMetadataVersion()) {
                            throw MetadataChangedException.INSTANCE;
                        }
                        ld.addColumnType(columnWriterIndex, metadata.getColumnType(columnWriterIndex));
                    } else if (!autoCreateNewColumns) {
                        throw newColumnsNotAllowed(columnNameUtf16, tud.getTableNameUtf16());
                    } else {
                        throw invalidColNameError(columnNameUtf16, tud.getTableNameUtf16());
                    }
                    break;
                case DUPLICATED_COLUMN:
                    // indicate to the second loop that writer index does not exist
                    ld.addColumnType(DUPLICATED_COLUMN, ColumnType.UNDEFINED);
                    break;
            }
        }

        TableWriter.Row r = writer.newRow(timestamp);
        try {
            for (int i = 0; i < entCount; i++) {
                final int colType = ld.getColumnType(i);
                final int columnIndex = ld.getColumnIndex(i);

                if (columnIndex < 0) {
                    continue;
                }

                final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
                switch (ent.getType()) {
                    case LineTcpParser.ENTITY_TYPE_TAG:
                    case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                        switch (colType) {
                            case ColumnType.STRING:
                                r.putStrUtf8(columnIndex, ent.getValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            case ColumnType.VARCHAR:
                                r.putVarchar(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "TAG", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_INTEGER: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.LONG:
                                r.putLong(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.INT: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Integer.MIN_VALUE && entityValue <= Integer.MAX_VALUE) {
                                    r.putInt(columnIndex, (int) entityValue);
                                } else if (entityValue == Numbers.LONG_NULL) {
                                    r.putInt(columnIndex, Numbers.INT_NULL);
                                } else {
                                    throw boundsError(entityValue, ColumnType.INT, tud.getTableNameUtf16(),
                                            writer.getMetadata().getColumnName(columnIndex));
                                }
                                break;
                            }
                            case ColumnType.SHORT: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                    r.putShort(columnIndex, (short) entityValue);
                                } else if (entityValue == Numbers.LONG_NULL) {
                                    r.putShort(columnIndex, (short) 0);
                                } else {
                                    throw boundsError(entityValue, ColumnType.SHORT, tud.getTableNameUtf16(),
                                            writer.getMetadata().getColumnName(columnIndex));
                                }
                                break;
                            }
                            case ColumnType.BYTE: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                    r.putByte(columnIndex, (byte) entityValue);
                                } else if (entityValue == Numbers.LONG_NULL) {
                                    r.putByte(columnIndex, (byte) 0);
                                } else {
                                    throw boundsError(entityValue, ColumnType.BYTE, tud.getTableNameUtf16(),
                                            writer.getMetadata().getColumnName(columnIndex));
                                }
                                break;
                            }
                            case ColumnType.TIMESTAMP:
                                r.putTimestamp(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.DATE:
                                r.putDate(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "INTEGER", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_FLOAT: {
                        switch (colType) {
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getFloatValue());
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, (float) ent.getFloatValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "FLOAT", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_STRING: {
                        final int geoHashBits = ColumnType.getGeoHashBits(colType);
                        final DirectUtf8Sequence entityValue = ent.getValue();
                        if (geoHashBits == 0) { // not geohash
                            switch (colType) {
                                case ColumnType.IPv4:
                                    try {
                                        int value = Numbers.parseIPv4Nl(entityValue);
                                        r.putInt(columnIndex, value);
                                    } catch (NumericException e) {
                                        throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.VARCHAR:
                                    r.putVarchar(columnIndex, entityValue);
                                    break;
                                case ColumnType.STRING:
                                    r.putStrUtf8(columnIndex, entityValue);
                                    break;
                                case ColumnType.CHAR:
                                    if (entityValue.size() == 1 && entityValue.byteAt(0) > -1) {
                                        r.putChar(columnIndex, (char) entityValue.byteAt(0));
                                    } else if (stringToCharCastAllowed) {
                                        int encodedResult = Utf8s.utf8CharDecode(entityValue);
                                        if (Numbers.decodeLowShort(encodedResult) > 0) {
                                            r.putChar(columnIndex, (char) Numbers.decodeHighShort(encodedResult));
                                        } else {
                                            throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                        }
                                    } else {
                                        throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.SYMBOL:
                                    r.putSymUtf8(columnIndex, entityValue);
                                    break;
                                case ColumnType.UUID:
                                    CharSequence asciiCharSequence = entityValue.asAsciiCharSequence();
                                    try {
                                        Uuid.checkDashesAndLength(asciiCharSequence);
                                        long uuidLo = Uuid.parseLo(asciiCharSequence);
                                        long uuidHi = Uuid.parseHi(asciiCharSequence);
                                        r.putLong128(columnIndex, uuidLo, uuidHi);
                                    } catch (NumericException e) {
                                        throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.LONG256:
                                    CharSequence cs = entityValue.asAsciiCharSequence();
                                    if (Numbers.extractLong256(cs, long256)) {
                                        r.putLong256(columnIndex, long256);
                                        break;
                                    }
                                    throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                default:
                                    throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                            }
                        } else {
                            long geoHash;
                            try {
                                DirectUtf8Sequence value = ent.getValue();
                                geoHash = GeoHashes.fromAsciiTruncatingNl(value.lo(), value.hi(), geoHashBits);
                            } catch (NumericException e) {
                                geoHash = GeoHashes.NULL;
                            }
                            r.putGeoHash(columnIndex, geoHash);
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        switch (colType) {
                            case ColumnType.LONG256:
                                r.putLong256Utf8(columnIndex, ent.getValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "LONG256", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        switch (colType) {
                            case ColumnType.BOOLEAN:
                                r.putBool(columnIndex, ent.getBooleanValue());
                                break;
                            case ColumnType.BYTE:
                                r.putByte(columnIndex, (byte) (ent.getBooleanValue() ? 1 : 0));
                                break;
                            case ColumnType.SHORT:
                                r.putShort(columnIndex, (short) (ent.getBooleanValue() ? 1 : 0));
                                break;
                            case ColumnType.INT:
                                r.putInt(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.LONG:
                                r.putLong(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "BOOLEAN", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.TIMESTAMP:
                                long timestampValue = ColumnType.getTimestampDriver(colType).from(ent.getLongValue(), ent.getUnit());
                                r.putTimestamp(columnIndex, timestampValue);
                                break;
                            case ColumnType.DATE:
                                TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
                                long dateValue = driver.toDate(driver.from(ent.getLongValue(), ent.getUnit()));
                                r.putTimestamp(columnIndex, dateValue);
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "TIMESTAMP", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_ARRAY:
                        ArrayView array = ent.getArray();
                        if (array.getType() != colType && !array.isNull()) {
                            throw castError(tud.getTableNameUtf16(), ColumnType.nameOf(array.getType()), colType, ent.getName());
                        }
                        r.putArray(columnIndex, array);
                        break;
                    default:
                        break; // unsupported types are ignored
                }
            }
            r.append();
            tud.commitIfMaxUncommittedRowsCountReached();
        } catch (CommitFailedException commitFailedException) {
            throw commitFailedException;
        } catch (CairoException th) {
            LOG.error().$("could not write line protocol measurement [tableName=")
                    .$(tud.getTableNameUtf16()).$(", message=").$safe(th.getFlyweightMessage()).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        } catch (Throwable th) {
            LOG.error().$("could not write line protocol measurement [tableName=")
                    .$(tud.getTableNameUtf16()).$(", message=").$safe(th.getMessage()).$(th).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        }
    }

    private byte getOverloadTimestampUnit(byte unit) {
        switch (unit) {
            case CommonUtils.TIMESTAMP_UNIT_NANOS:
            case CommonUtils.TIMESTAMP_UNIT_MILLIS:
            case CommonUtils.TIMESTAMP_UNIT_MICROS:
                return unit;
        }
        return timestampUnit;
    }
}
