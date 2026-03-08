/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import org.junit.Assert;

public class FuzzValidateSymbolFilterOperation implements FuzzTransactionOperation {
    private final String[] symbols;

    public FuzzValidateSymbolFilterOperation(String[] symbols) {
        this.symbols = symbols;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter,
                         int virtualTimestampIndex, LongList excludedTsIntervals) {
        if (symbols.length == 0) {
            return false;
        }
        TableToken tableToken = tableWriter.getTableToken();
        try (TableRecordMetadata metadata = engine.getTableMetadata(tableToken)) {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                if (metadata.getColumnType(i) == ColumnType.SYMBOL) {
                    validateSymbolIndex(engine, rnd, tableToken.getTableName(), metadata.getColumnName(i));
                    return false;
                }
            }
        } catch (Exception e) {
            // we intentionally catch and swallow all exceptions!
            // why? Fuzz tests use a failure injection and query compilation and execution can fail
            // with scary errors, such as I/O errors etc. That's expected and does not indicate a bug.
            // we are only interested in a correctness of a symbol lookup - so when a query executes
            // successfully only then we validate its results. nothing else matters.
            // Note: This won't catch AssertionError
        }
        return false;
    }

    private void validateSymbolIndex(CairoEngine engine, Rnd rnd, String tableName, String columnName) throws SqlException {
        String symbol = symbols[rnd.nextInt(symbols.length)];
        String sql = String.format(
                "SELECT %s, count(*) FROM %s WHERE %s = '%s' GROUP BY %s",
                columnName, tableName, columnName, symbol, columnName
        );

        try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1);
             RecordCursorFactory factory = engine.select(sql, context);
             RecordCursor cursor = factory.getCursor(context)) {

            int rowCount = 0;
            while (cursor.hasNext()) {
                rowCount++;
                if (rowCount > 1) {
                    Assert.fail("Query returned more than one row for symbol: " + symbol);
                }

                CharSequence resultSymbol = cursor.getRecord().getSymA(0);
                if (!Chars.equals(resultSymbol, symbol)) {
                    Assert.fail("Expected symbol '" + symbol + "' but got '" + resultSymbol + "'");
                }
            }
            // rowCount of 0 is valid (symbol doesn't exist)
            // rowCount of 1 is valid (symbol exists)
            // rowCount > 1 would indicate an index corruption issue
        }
    }
}