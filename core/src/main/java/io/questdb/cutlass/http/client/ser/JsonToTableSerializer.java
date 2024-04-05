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

package io.questdb.cutlass.http.client.ser;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cairo.ColumnType.*;

public class JsonToTableSerializer implements JsonParser, Mutable, QuietCloseable {
    private static final int STATE_DATA_ARRAY = 4;
    private static final int STATE_DATA_CELL = 10;
    private static final int STATE_DATA_ROW = 6;
    private static final int STATE_IGNORE_RECURSIVE = 12;
    private static final int STATE_MAIN_ATTR_NAMES = 1;
    private static final int STATE_METADATA_ARRAY = 3;
    private static final int STATE_METADATA_ATTR_COLUMN_NAME = 8;
    private static final int STATE_METADATA_ATTR_COLUMN_TYPE = 9;
    private static final int STATE_METADATA_ATTR_NAMES = 7;
    private static final int STATE_METADATA_OBJ = 5;
    private static final int STATE_METADATA_TIMESTAMP = 11;
    private static final int STATE_QUERY_TEXT = 2;
    private static final int STATE_START = 0;
    private final CreateTableModel createTableModel = CreateTableModel.FACTORY.newInstance();
    private final CairoEngine engine;
    private final JsonLexer lexer = new JsonLexer(1024, 1024);
    private final MemoryMARW mem = Vm.getMARWInstance();
    private final ExpressionNode partitionBy = ExpressionNode.FACTORY.newInstance();
    private final Path path = new Path();
    private final ExpressionNode tableName = ExpressionNode.FACTORY.newInstance();
    private final ExpressionNode timestampName = ExpressionNode.FACTORY.newInstance();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private int columnIndex;
    private String columnName;
    private int ignoreDepth = 0;
    private int ignoreReturnState;
    private TableWriter.Row row;
    private int state = STATE_START;
    private TableWriterAPI writer;

    public JsonToTableSerializer(CairoEngine engine) {
        this.engine = engine;
        // wire up the model
        createTableModel.setName(tableName);
        createTableModel.setTimestamp(timestampName);
        createTableModel.setPartitionBy(partitionBy);
    }

    @Override
    public void clear() {
        this.state = STATE_START;
        columnIndex = 0;
        columnName = null;
        lexer.clear();
        createTableModel.clear();
        createTableModel.setName(tableName);
        createTableModel.setTimestamp(timestampName);
        createTableModel.setPartitionBy(partitionBy);
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(mem);
        writer = Misc.free(writer);
    }

    @Override
    public void onEvent(int code, CharSequence tag, int position) throws JsonException {
        switch (code) {
            case JsonLexer.EVT_OBJ_START:
                switch (state) {
                    case STATE_START:
                        // expecting root object
                        state = STATE_MAIN_ATTR_NAMES;
                        // expect main attributes
                        // "query"
                        // "columns"
                        // "dataset"
                        break;
                    case STATE_METADATA_OBJ:
                        // expect metadata attribute
                        state = STATE_METADATA_ATTR_NAMES;
                        // expect:
                        // "name"
                        // "type"
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                switch (state) {
                    case STATE_MAIN_ATTR_NAMES: // the end
                        state = STATE_START;
                        break;
                    case STATE_METADATA_ATTR_NAMES: // column definition end
                        state = STATE_METADATA_OBJ;
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignore
                        ignoreDepth--;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_NAME:
                switch (state) {
                    case STATE_MAIN_ATTR_NAMES: // expecting any main attribute name
                        if (Chars.equals(tag, "query")) {
                            state = STATE_QUERY_TEXT; // expect query text
                        } else if (Chars.equals(tag, "columns")) {
                            state = STATE_METADATA_ARRAY; // expect metadata array
                        } else if (Chars.equals(tag, "dataset")) {
                            state = STATE_DATA_ARRAY; // expect data array
                        } else if (Chars.equals(tag, "timestamp")) {
                            state = STATE_METADATA_TIMESTAMP; // expect timestamp index
                        } else {
                            // ignore nested
                            state = STATE_IGNORE_RECURSIVE;
                            ignoreReturnState = STATE_MAIN_ATTR_NAMES;
                            ignoreDepth = 1;
                        }
                        break;
                    case STATE_METADATA_ATTR_NAMES: // metadata attribute
                        if (Chars.equals(tag, "name")) {
                            // column name
                            state = STATE_METADATA_ATTR_COLUMN_NAME;
                        } else if (Chars.equals(tag, "type")) {
                            // column type
                            state = STATE_METADATA_ATTR_COLUMN_TYPE;
                        } else {
                            assert false;
                        }
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (state) {
                    case STATE_QUERY_TEXT: // query text
                        // expect main attribute name
                        state = STATE_MAIN_ATTR_NAMES;
                        break;
                    case STATE_METADATA_ARRAY: // columns array
                        createTableModel.clear();
                        // going deeper
                        break;
                    case STATE_METADATA_ATTR_COLUMN_NAME: // column name
                        this.columnName = Chars.toString(tag);
                        // back to metadata attribute name
                        state = STATE_METADATA_ATTR_NAMES;
                        break;
                    case STATE_METADATA_ATTR_COLUMN_TYPE: // column type
                        try {
                            createTableModel.addColumn(columnName, ColumnType.typeOf(tag), 128);
                            state = STATE_METADATA_ATTR_NAMES;
                        } catch (SqlException e) {
                            throw new JsonException().put(e.getFlyweightMessage());
                        }
                        break;
                    case 11: // timestamp value
                        try {
                            int timestampIndex = Numbers.parseInt(tag);
                            if (timestampIndex != -1) {
                                timestampName.token = createTableModel.getColumnName(timestampIndex);
                                createTableModel.setTimestamp(timestampName);
                            } else {
                                createTableModel.setTimestamp(null);
                            }
                        } catch (NumericException e) {
                            throw JsonException.$(position, "invalid timestamp index: ").put(tag);
                        }
                        createTable();
                        // back to main attributes
                        state = STATE_MAIN_ATTR_NAMES;
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignoring
                        if (--ignoreDepth == 0) {
                            state = ignoreReturnState;
                        }
                        break;
                    default:
                        break;
                }
                break;
            case JsonLexer.EVT_ARRAY_START:
                switch (state) {
                    case STATE_METADATA_ARRAY: // metadata array
                        state = STATE_METADATA_OBJ; // expect metadata object
                        break;
                    case STATE_DATA_ARRAY: // data array
                        state = STATE_DATA_ROW; // expect metadata row
                        break;
                    case STATE_DATA_ROW: // data row array (nested into data array)
                        state = STATE_DATA_CELL;
                        // start processing from column 0
                        columnIndex = 0;
                        // timestamp value is not yet determined because
                        // we are parsing JSON in streaming mode

                        // value parser will later overwrite timestamp value by index
                        row = writer.newRowDeferTimestamp();
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                if (state == STATE_DATA_CELL) {
                    try {
                        switch (createTableModel.getColumnType(columnIndex)) {
                            case BOOLEAN:
                                row.putBool(columnIndex, (tag.charAt(0) | 32) == 't');
                                break;
                            case BYTE:
                                row.putByte(columnIndex, (byte) Numbers.parseInt(tag));
                                break;
                            case SHORT:
                                row.putShort(columnIndex, (short) Numbers.parseInt(tag));
                                break;
                            case CHAR:
                                row.putChar(columnIndex, tag.length() > 0 ? tag.charAt(0) : 0);
                                break;
                            case INT:
                                row.putInt(columnIndex, Numbers.parseInt(tag));
                                break;
                            case LONG:
                                row.putLong(columnIndex, Numbers.parseLong(tag));
                                break;
                            case DATE:
                                row.putDate(columnIndex, DateFormatUtils.parseUTCDate(tag));
                                break;
                            case TIMESTAMP:
                                row.putTimestamp(columnIndex, TimestampFormatUtils.parseUTCTimestamp(tag));
                                break;
                            case FLOAT:
                                row.putFloat(columnIndex, Numbers.parseFloat(tag));
                                break;
                            case DOUBLE:
                                row.putDouble(columnIndex, Numbers.parseDouble(tag));
                                break;
                            case STRING:
                                row.putStr(columnIndex, tag);
                                break;
                            case VARCHAR:
                                utf8Sink.clear();
                                utf8Sink.put(tag);
                                row.putVarchar(columnIndex, utf8Sink);
                                break;
                            case SYMBOL:
                                row.putSym(columnIndex, tag);
                                break;
                            case LONG256:
                                row.putLong256(columnIndex, tag);
                                break;
                            case GEOBYTE:
                            case GEOSHORT:
                            case GEOINT:
                            case GEOLONG:
                                row.putGeoStr(columnIndex, tag);
                                break;
                            case BINARY:
                                break;
                            case UUID:
                                row.putUuid(columnIndex, tag);
                                break;
                            default:
                                assert false;
                        }
                        columnIndex++;
                    } catch (NumericException e) {
                        row.cancel();
                        throw JsonException.$(position, "could not parse value [value=").put(tag).put(", type=").put(ColumnType.nameOf(createTableModel.getColumnType(columnIndex))).put(']');
                    }
                } else {
                    throw JsonException.$(position, "unexpected array value");
                }
                break;
            case JsonLexer.EVT_ARRAY_END:
                switch (state) {
                    case STATE_METADATA_OBJ: // metadata array end
                        // back to main attributes:
                        state = STATE_MAIN_ATTR_NAMES;
                        break;
                    case STATE_DATA_ROW: // data array end
                        state = STATE_MAIN_ATTR_NAMES;
                        writer.commit();
                        break;
                    case STATE_DATA_CELL: // data row array end
                        row.append();
                        state = STATE_DATA_ROW; // expect data row array
                        break;
                    case STATE_IGNORE_RECURSIVE: // ignore
                        ignoreDepth--;
                        break;
                    default:
                        assert false;
                }
                break;
        }
    }

    public void parse(long lo, long hi) throws JsonException {
        lexer.parse(lo, hi, this);
    }

    private void createTable() {
        tableName.token = "hello123";
        createTableModel.setWalEnabled(true);
        partitionBy.token = "HOUR";
        writer = engine.getWalWriter(engine.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, createTableModel, false));
    }
}
