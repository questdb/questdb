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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

public class JsonDataParser implements JsonParser, Mutable, QuietCloseable {
    private final CreateTableModel createTableModel = CreateTableModel.FACTORY.newInstance();
    private final CairoEngine engine;
    private final JsonLexer lexer = new JsonLexer(1024, 1024);
    private final MemoryMARW mem = Vm.getMARWInstance();
    private final ExpressionNode tableName = ExpressionNode.FACTORY.newInstance();
    private final Path path = new Path();
    private int columnIndex;
    private String columnName;
    private int state = 0;

    public JsonDataParser(CairoEngine engine) {
        this.engine = engine;
        // wire up the model
        createTableModel.setName(tableName);
    }

    @Override
    public void clear() {
        this.state = 0;
        columnIndex = 0;
        columnName = null;
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(mem);
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
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                switch (state) {
                    case 7: // column definition end
                        state = 5;
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
                        } else {
                            assert false;
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
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (state) {
                    case 2: // query text
                        System.out.println("Query text: " + tag);
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
                        break;
                    default:
                        assert false;
                }
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                switch (state) {
                    case 10: // column value
                        switch (createTableModel.getColumnType(columnIndex++)) {
                            case ColumnType.INT:

                        }
                        break;
                    default:
                        assert false;

                }
            case JsonLexer.EVT_ARRAY_END:
                switch (state) {
                    case 5: // metadata array end
                        tableName.token = "hello123";
                        createTableModel.setWalEnabled(true);
                        engine.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, createTableModel, false);
                        // back to main attributes:
                        state = 1;
                        break;
                    default:
                        assert false;
                }
                break;
        }
        System.out.println(code);
    }

    public void parse(long lo, long hi) throws JsonException {
        lexer.parse(lo, hi, this);
    }
}
