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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.jit.JitUtil;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalJitGenerationTest extends AbstractCairoTest {
    @Test
    public void testIntegerInListsPreserveNullsAndLongWidths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jf_rows(id INT,n LONG)");
            execute("INSERT INTO jf_rows VALUES (null,null),(1,1),(2,2147483648),(3,-2147483648),(4,4)");
            final int oldMode = sqlExecutionContext.getJitMode();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int path = 0; path < 2; path++) {
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE id IN(1,2,4)", "id\n1\n2\n4\n", mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE id NOT IN(1,2,4)", "id\nnull\n3\n", mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE id IN(null,2)", "id\nnull\n2\n", mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE id IN(-2147483648L,4L)", "id\n4\n", mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE id IN(2147483648L,1L)", "id\n1\n", mode);
                        assertCompiled(compiler, "SELECT id FROM jf_rows WHERE n IN(-2147483648L,null)", "id\nnull\n3\n", mode);
                        bindVariableService.setLong(0, 2);
                        try (RecordCursorFactory factory = compiler.compile("SELECT id FROM jf_rows WHERE id IN(1,$1)",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported(), factory.usesCompiledFilter());
                            compiler.clear();
                            assertResult(factory, "id\n1\n2\n");
                            bindVariableService.setLong(0, Numbers.LONG_NULL);
                            assertResult(factory, "id\nnull\n1\n");
                            bindVariableService.setLong(0, 2147483648L);
                            assertResult(factory, "id\n1\n");
                        }
                        bindVariableService.clear();
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testTimestampLongArithmeticUsesExistingNullAndOverflowRules() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE jf_rows(id INT,ts " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ",n LONG)");
                execute("INSERT INTO jf_rows VALUES (1,'1970-01-01',1),(2,'1970-01-01',null),(3,null,1),"
                        + "(4,'1970-01-01',9223372036854775807),(5,'1970-01-01',-1)");
                final int oldMode = sqlExecutionContext.getJitMode();
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    for (int path = 0; path < 2; path++) {
                        for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                            sqlExecutionContext.setJitMode(mode);
                            final String point = nanos ? "'1970-01-01T00:00:00.000000000Z'" : "'1970-01-01T00:00:00.000000Z'";
                            assertCompiled(compiler, "SELECT id FROM jf_rows WHERE ts+n>" + point, "id\n1\n4\n", mode, false);
                            assertCompiled(compiler, "SELECT id FROM jf_rows WHERE ts+1L<" + point, "id\n", mode);
                            assertCompiled(compiler, "SELECT id FROM jf_rows WHERE ts-1L<" + point, "id\n1\n2\n4\n5\n", mode);
                            assertCompiled(compiler, "SELECT id FROM jf_rows WHERE ts+n+1L<" + point, "id\n", mode, false);
                            assertCompiled(compiler, "SELECT id FROM jf_rows WHERE ts+n+2L<" + point, "id\n4\n", mode, false);
                        }
                    }
                } finally {
                    sqlExecutionContext.setJitMode(oldMode);
                }
                execute("DROP TABLE jf_rows");
            });
        }
    }

    @Test
    public void testJitDeclineAndRecoveryAcrossModesAndCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jf_rows(id INT,label STRING)");
            execute("INSERT INTO jf_rows VALUES(1,'A'),(2,'B'),(3,'A'),(4,null)");
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            sqlExecutionContext.setParallelFilterEnabled(true);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int path = 0; path < 2; path++) {
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        bindVariableService.setInt(0, 1);
                        try (RecordCursorFactory declined = compiler.compile(
                                "SELECT id FROM jf_rows WHERE nullif(id,2)>$1 LIMIT 2", sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertFalse(declined.usesCompiledFilter());
                            assertResult(declined, "id\n3\n4\n");
                        }
                        RecordCursorFactory retained = null;
                        try {
                            retained = compiler.compile("SELECT id FROM jf_rows WHERE id>$1 LIMIT 2", sqlExecutionContext).getRecordCursorFactory();
                            Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported(), retained.usesCompiledFilter());
                            compiler.clear();
                            bindVariableService.setInt(0, 2);
                            assertResult(retained, "id\n3\n4\n");
                            bindVariableService.setInt(0, 3);
                            assertResult(retained, "id\n4\n");
                        } finally {
                            Misc.free(retained);
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testThreadUnsafeJavaFallbackKeepsNativeChildrenAndLimitSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jf_rows(id INT,label STRING)");
            execute("INSERT INTO jf_rows VALUES(1,'A'),(2,'B'),(3,'A'),(4,'A'),(5,null)");
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setParallelFilterEnabled(true);
            try {
                for (int path = 0; path < 2; path++) {
                    RecordCursorFactory retained = null;
                    try {
                        bindVariableService.setStr(0, "a");
                        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                            retained = compiler.compile("SELECT id FROM jf_rows WHERE lower(label)=$1 AND id IN (1,3,4) LIMIT -2",
                                    sqlExecutionContext).getRecordCursorFactory();
                            Assert.assertFalse(retained.usesCompiledFilter());
                        }
                        assertResult(retained, "id\n3\n4\n");
                        bindVariableService.setStr(0, "b");
                        assertResult(retained, "id\n");
                        bindVariableService.setStr(0, "a");
                        assertResult(retained, "id\n3\n4\n");
                    } finally {
                        Misc.free(retained);
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
            }
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void assertCompiled(SqlCompilerImpl compiler, String query, String expected, int mode) throws Exception {
        assertCompiled(compiler, query, expected, mode, true);
    }

    private void assertCompiled(SqlCompilerImpl compiler, String query, String expected, int mode, boolean supported) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertEquals(query, supported && mode != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported(), factory.usesCompiledFilter());
            assertResult(factory, expected);
        }
    }
}
