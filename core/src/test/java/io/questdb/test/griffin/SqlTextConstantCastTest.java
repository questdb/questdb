/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.jit.JitUtil;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class SqlTextConstantCastTest extends AbstractCairoTest {
    @Test
    public void testCharCastsPreserveDecodedValues() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> values = new ObjList<>();
            values.add("'");
            values.add("界");
            values.add("A");
            values.add(null);
            final ObjList<String> types = textTypes();
            for (int i = 0; i < values.size(); i++) {
                for (int j = 0; j < types.size(); j++) {
                    final String type = types.getQuick(j);
                    assertValue("CAST(CAST(" + literal(values.getQuick(i)) + " AS CHAR) AS " + type + ")",
                            ColumnType.typeOf(type), values.getQuick(i));
                }
            }
        });
    }

    @Test
    public void testReplacementAndFormattingPreserveDecodedValues() throws Exception {
        assertMemoryLeak(() -> {
            assertValue("replace(CAST('x界x' AS VARCHAR),CAST('x' AS VARCHAR),CAST(" + literal("'") + " AS VARCHAR))",
                    ColumnType.VARCHAR, "'界'");
            assertValue("to_str(CAST(0 AS DATE)," + literal("''yyyy''") + ")", ColumnType.STRING, "''1970''");
            assertValue("to_str(CAST(0 AS TIMESTAMP)," + literal("''yyyy''") + ")", ColumnType.STRING, "''1970''");
        });
    }

    @Test
    public void testTextCastsPreserveDecodedValues() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> values = new ObjList<>();
            values.add("");
            values.add("'");
            values.add("'edge'");
            values.add("O'Reilly");
            values.add("'界🙂'");
            values.add("TRUE");
            values.add("FaLsE");
            values.add(null);
            final ObjList<String> types = textTypes();
            for (int i = 0; i < values.size(); i++) {
                for (int source = 0; source < types.size(); source++) {
                    for (int target = 0; target < types.size(); target++) {
                        if (source != target) {
                            final String type = types.getQuick(target);
                            assertValue("CAST(CAST(" + literal(values.getQuick(i)) + " AS " + types.getQuick(source) + ") AS " + type + ")",
                                    ColumnType.typeOf(type), values.getQuick(i));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testUnknownJitSymbolConstantPreservesDecodedValue() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE text_constant_jit(id INT,s SYMBOL)");
            execute("INSERT INTO text_constant_jit VALUES (1,'other')");
            final int previousMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try {
                try (RecordCursorFactory factory = select("SELECT id FROM text_constant_jit WHERE s=" + literal("'edge'") + " OR id<0")) {
                    Assert.assertTrue(factory.usesCompiledFilter());
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n");
                    execute("INSERT INTO text_constant_jit VALUES (2," + literal("'edge'") + ")");
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n2\n");
                }
            } finally {
                sqlExecutionContext.setJitMode(previousMode);
            }
        });
    }

    private static String literal(String value) {
        return value == null ? "null" : "'" + value.replace("'", "''") + "'";
    }

    private static ObjList<String> textTypes() {
        final ObjList<String> types = new ObjList<>();
        types.add("STRING");
        types.add("VARCHAR");
        types.add("SYMBOL");
        return types;
    }

    private void assertValue(String expression, int type, String expected) throws Exception {
        final String sql = "SELECT " + expression + " AS value";
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, type, factory.getMetadata().getColumnType(0));
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                final Record record = cursor.getRecord();
                switch (type) {
                    case ColumnType.STRING:
                        TestUtils.assertEquals(expected, record.getStrA(0));
                        TestUtils.assertEquals(expected, record.getStrB(0));
                        break;
                    case ColumnType.VARCHAR:
                        if (expected == null) {
                            Assert.assertNull(record.getVarcharA(0));
                            Assert.assertNull(record.getVarcharB(0));
                        } else {
                            final Utf8String value = new Utf8String(expected);
                            Assert.assertTrue(sql, Utf8s.equals(value, record.getVarcharA(0)));
                            Assert.assertTrue(sql, Utf8s.equals(value, record.getVarcharB(0)));
                        }
                        break;
                    case ColumnType.SYMBOL:
                        TestUtils.assertEquals(expected, record.getSymA(0));
                        TestUtils.assertEquals(expected, record.getSymB(0));
                        Assert.assertEquals(sql, expected == null ? SymbolTable.VALUE_IS_NULL : 0, record.getInt(0));
                        TestUtils.assertEquals(expected, cursor.getSymbolTable(0).valueOf(record.getInt(0)));
                        break;
                    default:
                        Assert.fail(sql);
                }
                Assert.assertFalse(sql, cursor.hasNext());
            }
        }
    }
}
