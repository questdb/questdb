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

package io.questdb.client.ser;

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

import static io.questdb.cairo.ColumnType.*;

public class JsonDataParser implements JsonParser, Mutable, QuietCloseable {
    private final CreateTableModel createTableModel = CreateTableModel.FACTORY.newInstance();
    private final CairoEngine engine;
    private final JsonLexer lexer = new JsonLexer(1024, 1024);
    private final MemoryMARW mem = Vm.getMARWInstance();
    private final ExpressionNode partitionBy = ExpressionNode.FACTORY.newInstance();
    private final Path path = new Path();
    private final ExpressionNode tableName = ExpressionNode.FACTORY.newInstance();
    private final ExpressionNode timestampName = ExpressionNode.FACTORY.newInstance();
    private int columnIndex;
    private String columnName;
    private int ignoreDepth = 0;
    private int ignoreReturnState;
    private TableWriter.Row row;
    private int state = 0;
    private TableWriterAPI writer;

    public JsonDataParser(CairoEngine engine) {
        this.engine = engine;
        // wire up the model
        createTableModel.setName(tableName);
        createTableModel.setTimestamp(timestampName);
        createTableModel.setPartitionBy(partitionBy);
    }

    @Override
    public void clear() {
        this.state = 0;
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
                    case 0:
                        // expecting root object
                        state = 1;
                        // expect main attributes
                        // "query"
                        // "columns"
                        // "dataset"
                        break;
                    case 5:
                        // expect metadata attribute
                        state = 7;
                        // expect:
                        // "name"
                        // "type"
                        break;
                    case 12: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                switch (state) {
                    case 1: // the end
                        System.out.println("done");
                        break;
                    case 7: // column definition end
                        state = 5;
                        break;
                    case 12: // ignore
                        ignoreDepth--;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_NAME:
                switch (state) {
                    case 1: // expecting any main attribute name
                        if (Chars.equals(tag, "query")) {
                            state = 2; // expect query text
                        } else if (Chars.equals(tag, "columns")) {
                            state = 3; // expect metadata array
                        } else if (Chars.equals(tag, "dataset")) {
                            state = 4; // expect data array
                        } else if (Chars.equals(tag, "timestamp")) {
                            state = 11; // expect timestamp index
                        } else {
                            // ignore nested
                            state = 12;
                            ignoreReturnState = 1;
                            ignoreDepth = 1;
                        }
                        break;
                    case 7: // metadata attribute
                        if (Chars.equals(tag, "name")) {
                            // column name
                            state = 8;
                        } else if (Chars.equals(tag, "type")) {
                            // column type
                            state = 9;
                        } else {
                            assert false;
                        }
                        break;
                    case 12: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (state) {
                    case 2: // query text
                        // expect main attribute name
                        state = 1;
                        break;
                    case 3: // columns array
                        createTableModel.clear();
                        // going deeper
                        break;
                    case 8: // column name
                        this.columnName = Chars.toString(tag);
                        // back to metadata attribute name
                        state = 7;
                        break;
                    case 9: // column type
                        try {
                            createTableModel.addColumn(columnName, ColumnType.typeOf(tag), 128);
                            state = 7;
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
                        state = 1;
                        break;
                    case 12: // ignoring
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
                    case 3: // metadata array
                        state = 5; // expect metadata object
                        break;
                    case 4: // data array
                        state = 6; // expect metadata row
                        break;
                    case 6: // data row array (nested into data array)
                        state = 10;
                        // start processing from column 0
                        columnIndex = 0;
                        // timestamp value is not yet determined because
                        // we are parsing JSON in streaming mode

                        // value parser will later overwrite timestamp value by index
                        row = writer.newRowDeferTimestamp();
                        break;
                    case 12: // ignore
                        ignoreDepth++;
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                switch (state) {
                    case 10: // column value
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
                        break;
                    default:
                        assert false;
                        break;
                }
                break;
            case JsonLexer.EVT_ARRAY_END:
                switch (state) {
                    case 5: // metadata array end
                        // back to main attributes:
                        state = 1;
                        break;
                    case 6: // data array end
                        state = 1;
                        writer.commit();
                        break;
                    case 10: // data row array end
                        row.append();
                        state = 6; // expect data row array
                        break;
                    case 12: // ignore
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
