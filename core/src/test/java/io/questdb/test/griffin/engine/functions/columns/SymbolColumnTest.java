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

package io.questdb.test.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The record's text and the dictionary deliberately disagree, so each assertion proves which side
 * the column reads. A non-static column reads the record text (getSymA/getSymB) without minting a
 * key; a static column keeps the integer fast path (dictionary valueOf(getInt)). Both must agree on
 * a well-formed cursor - this pins the getSymbol read-path split introduced for lazy symbol casts.
 */
public class SymbolColumnTest extends AbstractCairoTest {

    @Test
    public void testNonStaticColumnReadsRecordTextNotDictionary() {
        SymbolColumn column = new SymbolColumn(0, false);
        column.init(sourceOf(dictionaryTable()), sqlExecutionContext);
        TestUtils.assertEquals("recordTextA", column.getSymbol(valueRecord()));
        TestUtils.assertEquals("recordTextB", column.getSymbolB(valueRecord()));
        Assert.assertEquals(7, column.getInt(valueRecord()));
        Assert.assertNull(column.getSymbol(nullRecord()));
        Assert.assertNull(column.getSymbolB(nullRecord()));
        column.close();
    }

    @Test
    public void testStaticColumnResolvesThroughDictionary() {
        SymbolColumn column = new SymbolColumn(0, true);
        column.init(sourceOf(staticDictionaryTable()), sqlExecutionContext);
        TestUtils.assertEquals("dictValueOf7", column.getSymbol(valueRecord()));
        TestUtils.assertEquals("dictValueBOf7", column.getSymbolB(valueRecord()));
        Assert.assertNull(column.getSymbol(nullRecord()));
        Assert.assertNull(column.getSymbolB(nullRecord()));
        column.close();
    }

    @Test
    public void testSupportsKeyValueAccessFollowsTheBoundDictionary() {
        // QwpResultBatchBuffer reads this flag to decide whether egress may ship a value once per
        // key, so it has to follow the dictionary actually bound rather than the column's declared
        // staticness. It is also read before init() binds one, where a safe false is required.
        SymbolColumn uninitialised = new SymbolColumn(0, false);
        Assert.assertFalse("an unbound column must not claim the key path", uninitialised.supportsKeyValueAccess());

        SymbolColumn overStatic = new SymbolColumn(0, true);
        overStatic.init(sourceOf(staticDictionaryTable()), sqlExecutionContext);
        Assert.assertTrue(overStatic.supportsKeyValueAccess());
        overStatic.close();

        SymbolColumn overDynamic = new SymbolColumn(0, false);
        overDynamic.init(sourceOf(dictionaryTable()), sqlExecutionContext);
        Assert.assertFalse(overDynamic.supportsKeyValueAccess());
        overDynamic.close();
    }

    private static SymbolTable dictionaryTable() {
        return new SymbolTable() {
            @Override
            public CharSequence valueBOf(int key) {
                return "dictValueBOf" + key;
            }

            @Override
            public CharSequence valueOf(int key) {
                return "dictValueOf" + key;
            }
        };
    }

    private static Record nullRecord() {
        return new Record() {
            @Override
            public int getInt(int col) {
                return SymbolTable.VALUE_IS_NULL;
            }

            @Override
            public CharSequence getSymA(int col) {
                return null;
            }

            @Override
            public CharSequence getSymB(int col) {
                return null;
            }
        };
    }

    private static SymbolTableSource sourceOf(SymbolTable symbolTable) {
        return new SymbolTableSource() {
            @Override
            public SymbolTable getSymbolTable(int columnIndex) {
                return symbolTable;
            }

            @Override
            public SymbolTable newSymbolTable(int columnIndex) {
                return symbolTable;
            }
        };
    }

    private static StaticSymbolTable staticDictionaryTable() {
        return new StaticSymbolTable() {
            @Override
            public boolean containsNullValue() {
                return false;
            }

            @Override
            public int getSymbolCount() {
                return 8;
            }

            @Override
            public int keyOf(CharSequence value) {
                return VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return key == SymbolTable.VALUE_IS_NULL ? null : "dictValueBOf" + key;
            }

            @Override
            public CharSequence valueOf(int key) {
                return key == SymbolTable.VALUE_IS_NULL ? null : "dictValueOf" + key;
            }
        };
    }

    private static Record valueRecord() {
        return new Record() {
            @Override
            public int getInt(int col) {
                return 7;
            }

            @Override
            public CharSequence getSymA(int col) {
                return "recordTextA";
            }

            @Override
            public CharSequence getSymB(int col) {
                return "recordTextB";
            }
        };
    }
}
