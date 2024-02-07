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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cutlass.line.LineTcpTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.line.tcp.LineProtocolException.*;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.DUPLICATED_COLUMN;

public class LineWalAppender {
    private static final Log LOG = LogFactory.getLog(LineWalAppender.class);
    private final boolean autoCreateNewColumns;
    private final int maxFileNameLength;
    private final MicrosecondClock microsecondClock;
    private final boolean stringToCharCastAllowed;
    private LineTcpTimestampAdapter timestampAdapter;

    public LineWalAppender(boolean autoCreateNewColumns, boolean stringToCharCastAllowed, LineTcpTimestampAdapter timestampAdapter, int maxFileNameLength, MicrosecondClock microsecondClock) {
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.stringToCharCastAllowed = stringToCharCastAllowed;
        this.timestampAdapter = timestampAdapter;
        this.maxFileNameLength = maxFileNameLength;
        this.microsecondClock = microsecondClock;
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
        switch (precision) {
            case LineTcpParser.ENTITY_UNIT_NANO:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_NANO_INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MICRO:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_MICRO_INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MILLI:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_MILLI_INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_SECOND:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_SECOND_INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MINUTE:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_MINUTE_INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_HOUR:
                timestampAdapter = LineTcpTimestampAdapter.DEFAULT_TS_HOUR_INSTANCE;
                break;
            default:
                throw new UnsupportedOperationException("precision: " + precision);
        }
    }

    private void appendToWal0(
            SecurityContext securityContext,
            LineTcpParser parser,
            TableUpdateDetails tud
    ) throws CommitFailedException, MetadataChangedException {

        // pass 1: create all columns that do not exist
        final TableUpdateDetails.ThreadLocalDetails ld = tud.getThreadLocalDetails(0); // IO thread id is not relevant
        ld.resetStateIfNecessary();
        ld.clearColumnTypes();

        final TableWriterAPI writer = tud.getWriter();
        assert writer.supportsMultipleWriters();
        TableRecordMetadata metadata = writer.getMetadata();
        long initialMetadataVersion = ld.getMetadataVersion();

        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp, parser.getTimestampUnit());
        } else {
            timestamp = microsecondClock.getTicks();
        }

        final int entCount = parser.getEntityCount();
        for (int i = 0; i < entCount; i++) {
            final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
            int columnWriterIndex = ld.getColumnWriterIndex(ent.getName(), parser.hasNonAsciiChars(), metadata);

            switch (columnWriterIndex) {
                default:
                    final int columnType = metadata.getColumnType(columnWriterIndex);
                    if (columnType > -1) {
                        if (columnWriterIndex == tud.getTimestampIndex()) {
                            timestamp = timestampAdapter.getMicros(ent.getLongValue(), ent.getUnit());
                            ld.addColumnType(DUPLICATED_COLUMN, ColumnType.UNDEFINED);
                        } else {
                            ld.addColumnType(columnWriterIndex, metadata.getColumnType(columnWriterIndex));
                        }
                        break;
                    } else {
                        // column has been deleted from the metadata, but it is in our utf8 cache
                        ld.removeFromCaches(ent.getName(), parser.hasNonAsciiChars());
                        // act as if we did not find this column and fall through
                    }
                case COLUMN_NOT_FOUND:
                    final String columnNameUtf16 = ld.getColNameUtf16();
                    if (autoCreateNewColumns && TableUtils.isValidColumnName(columnNameUtf16, maxFileNameLength)) {
                        columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                        if (columnWriterIndex < 0) {
                            securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                            try {
                                writer.addColumn(columnNameUtf16, ld.getColumnType(ld.getColNameUtf8(), ent.getType()), securityContext);
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                            } catch (CairoException e) {
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                                if (columnWriterIndex < 0) {
                                    // the column is still not there, something must be wrong
                                    throw e;
                                }
                                // all good, someone added the column concurrently
                            }
                        }
                        if (ld.getMetadataVersion() != initialMetadataVersion) {
                            // Restart the whole line,
                            // some columns can be deleted or renamed in tud.commit and ww.addColumn calls
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
                int colTypeAndIndex = ld.getColumnType(i);
                int colType = Numbers.decodeLowShort(colTypeAndIndex);
                int columnIndex = Numbers.decodeHighShort(colTypeAndIndex);

                if (columnIndex < 0) {
                    continue;
                }

                final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
                switch (ent.getType()) {
                    case LineTcpParser.ENTITY_TYPE_TAG:
                    case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                        // parser would reject this condition based on config
                        if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                            r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                        } else {
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
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putInt(columnIndex, Numbers.INT_NaN);
                                } else {
                                    throw boundsError(entityValue, ColumnType.INT, tud.getTableNameUtf16(), writer.getMetadata().getColumnName(columnIndex));
                                }
                                break;
                            }
                            case ColumnType.SHORT: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                    r.putShort(columnIndex, (short) entityValue);
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putShort(columnIndex, (short) 0);
                                } else {
                                    throw boundsError(entityValue, ColumnType.SHORT, tud.getTableNameUtf16(), writer.getMetadata().getColumnName(columnIndex));
                                }
                                break;
                            }
                            case ColumnType.BYTE: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                    r.putByte(columnIndex, (byte) entityValue);
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putByte(columnIndex, (byte) 0);
                                } else {
                                    throw boundsError(entityValue, ColumnType.BYTE, tud.getTableNameUtf16(), writer.getMetadata().getColumnName(columnIndex));
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
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "INTEGER", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_FLOAT: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getFloatValue());
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, (float) ent.getFloatValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
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
                            switch (ColumnType.tagOf(colType)) {
                                case ColumnType.IPv4:
                                    try {
                                        int value = Numbers.parseIPv4Nl(entityValue);
                                        r.putInt(columnIndex, value);
                                    } catch (NumericException e) {
                                        throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.STRING:
                                    r.putStrUtf8(columnIndex, entityValue, parser.hasNonAsciiChars());
                                    break;
                                case ColumnType.CHAR:
                                    if (entityValue.size() == 1 && entityValue.byteAt(0) > -1) {
                                        r.putChar(columnIndex, (char) entityValue.byteAt(0));
                                    } else if (stringToCharCastAllowed) {
                                        int encodedResult = Utf8s.utf8CharDecode(entityValue.lo(), entityValue.hi());
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
                                    r.putSymUtf8(columnIndex, entityValue, parser.hasNonAsciiChars());
                                    break;
                                case ColumnType.UUID:
                                    r.putUuidUtf8(columnIndex, entityValue);
                                    break;
                                default:
                                    throw castError(tud.getTableNameUtf16(), "STRING", colType, ent.getName());
                            }
                        } else {
                            long geoHash;
                            try {
                                DirectUtf8Sequence value = ent.getValue();
                                geoHash = GeoHashes.fromStringTruncatingNl(value.lo(), value.hi(), geoHashBits);
                            } catch (NumericException e) {
                                geoHash = GeoHashes.NULL;
                            }
                            r.putGeoHash(columnIndex, geoHash);
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.LONG256:
                                r.putLong256Utf8(columnIndex, ent.getValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "LONG256", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        switch (ColumnType.tagOf(colType)) {
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
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "BOOLEAN", colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.TIMESTAMP:
                                long timestampValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(ent.getLongValue(), ent.getUnit());
                                r.putTimestamp(columnIndex, timestampValue);
                                break;
                            case ColumnType.DATE:
                                long dateValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(ent.getLongValue(), ent.getUnit());
                                r.putTimestamp(columnIndex, dateValue / 1000);
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError(tud.getTableNameUtf16(), "TIMESTAMP", colType, ent.getName());
                        }
                        break;
                    }
                    default:
                        break; // unsupported types are ignored
                }
            }
            r.append();
            tud.commitIfMaxUncommittedRowsCountReached();
        } catch (CommitFailedException commitFailedException) {
            throw commitFailedException;
        } catch (CairoException th) {
            LOG.error().$("could not write line protocol measurement [tableName=").$(tud.getTableNameUtf16()).$(", message=").$(th.getFlyweightMessage()).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        } catch (Throwable th) {
            LOG.error().$("could not write line protocol measurement [tableName=").$(tud.getTableNameUtf16()).$(", message=").$(th.getMessage()).$(th).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        }
    }
}
