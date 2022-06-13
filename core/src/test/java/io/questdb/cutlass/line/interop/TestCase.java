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

package io.questdb.cutlass.line.interop;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.util.ArrayList;
import java.util.List;

public final class TestCase {
    private final String name;
    private final String table;
    private final List<Symbol> symbols;
    private final List<Column> columns;
    private final Result expectedResult;

    public String getName() {
        return name;
    }

    public String getTable() {
        return table;
    }

    public List<Symbol> getSymbols() {
        return symbols;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Result getExpectedResult() {
        return expectedResult;
    }

    private TestCase(String name, String table, List<Symbol> symbols, List<Column> columns, Result expectedResult) {
        this.name = name;
        this.table = table;
        this.symbols = symbols;
        this.columns = columns;
        this.expectedResult = expectedResult;
    }

    public static TestCase fromJson(JsonObject jsonObject) {
        String name = jsonObject.get("testName").asString();
        String table = jsonObject.get("table").asString();

        JsonArray jsonSymbolArray = jsonObject.get("symbols").asArray();
        List<Symbol> symbols = new ArrayList<>(jsonSymbolArray.size());
        for (JsonValue jsonSymbol : jsonSymbolArray) {
            Symbol symbol = Symbol.fromJson(jsonSymbol.asObject());
            symbols.add(symbol);
        }

        JsonArray jsonColumnArray = jsonObject.get("columns").asArray();
        List<Column> columns = new ArrayList<>(jsonColumnArray.size());
        for (JsonValue jsonColumn : jsonColumnArray) {
            Column column = Column.fromJson(jsonColumn.asObject());
            columns.add(column);
        }
        Result result = Result.fromJson(jsonObject.get("result").asObject());

        return new TestCase(name, table, symbols, columns, result);
    }
}
