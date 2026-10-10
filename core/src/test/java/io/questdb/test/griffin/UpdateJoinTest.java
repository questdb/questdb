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

import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UpdateJoinTest extends AbstractCairoTest {
    @Test
    public void testCrossJoinKeepsFirstMatchAndTargetRowOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s WHERE t.id<3", 2,
                        """
                                SelectedRecord
                                    Cross Join
                                        Async Filter workers: 1
                                          filter: id<3
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_target
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t11\tA\n2\t11\tB\n3\t0\tC\n");
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s", 3,
                        """
                                SelectedRecord
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t11\tA\n2\t11\tB\n3\t11\tC\n");
            }
            dropRows();
        });
    }

    @Test
    public void testDeletedColumnsKeepTargetWriterAndSourceReaderLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("ALTER TABLE uj_target DROP COLUMN unused");
            execute("ALTER TABLE uj_source DROP COLUMN unused");
            execute("ALTER TABLE uj_source ADD COLUMN extra INT");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s WHERE t.id=s.id", 2,
                        """
                                SelectedRecord
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t11\tA\n2\t0\tB\n3\t33\tC\n");
            }
            dropRows();
        });
    }

    @Test
    public void testDerivedAndCteSourcesCanContainJoins() throws Exception {
        assertMemoryLeak(() -> {
            final String source = "SELECT d.id,d.v+x.delta v FROM uj_source d JOIN uj_extra x ON d.id=x.id";
            final String[] updates = {
                    "UPDATE uj_target t SET copied=s.v FROM (" + source + ") s WHERE t.id=s.id",
                    "WITH s AS (" + source + ") UPDATE uj_target t SET copied=s.v FROM s WHERE t.id=s.id"
            };
            for (String sql : updates) {
                createRows();
                execute("CREATE TABLE uj_extra(id INT,delta INT)");
                execute("INSERT INTO uj_extra VALUES(1,1),(3,3)");
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    assertUpdate(compiler, sql, 2,
                            """
                                    SelectedRecord
                                        Hash Join
                                          condition: s.id=t.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_target
                                            Hash
                                                VirtualRecord
                                                  functions: [d.id,d.v+x.delta]
                                                    Hash Join Light
                                                      condition: x.id=d.id
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: uj_source
                                                        Hash
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: uj_extra
                                    """);
                    assertRows(compiler, "id\tcopied\tlabel\n1\t12\tA\n2\t0\tB\n3\t36\tC\n");
                }
                execute("DROP TABLE uj_extra");
                dropRows();
            }
        });
    }

    @Test
    public void testNoMatchAndDuplicateMatchesPreserveAffectedRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("INSERT INTO uj_source VALUES('duplicate',1,999L,111,'later')");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s WHERE t.id=s.id AND s.id<0", 0,
                        """
                                SelectedRecord
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: id<0
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t0\tA\n2\t0\tB\n3\t0\tC\n");
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s WHERE t.id=s.id", 2,
                        """
                                SelectedRecord
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t111\tA\n2\t0\tB\n3\t33\tC\n");
            }
            dropRows();
        });
    }

    @Test
    public void testSelfJoinUsesTargetRowIdsWithComputedAssignments() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.id*10 FROM uj_target s WHERE t.id=s.id", 3,
                        """
                                VirtualRecord
                                  functions: [s.id*10]
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_target
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t10\tA\n2\t20\tB\n3\t30\tC\n");
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.id+t.id FROM uj_target s WHERE t.id=s.id", 3,
                        """
                                VirtualRecord
                                  functions: [s.id+t.id]
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_target
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t2\tA\n2\t4\tB\n3\t6\tC\n");
            }
            dropRows();
        });
    }

    @Test
    public void testTargetMetadataIsIndependentOfSourceColumnNamesAndTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler,
                        "UPDATE uj_target AS t SET COPIED=s.v,label=s.label FROM uj_source s WHERE t.id=s.id", 2,
                        """
                                SelectedRecord
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t11\tX\n2\t0\tB\n3\t33\tZ\n");
                bindVariableService.setInt(0, 44);
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=$1 FROM uj_source s WHERE t.id=s.id AND t.id=1", 1,
                        """
                                VirtualRecord
                                  functions: [$0::int]
                                    Hash Join Light
                                      condition: s.id=t.id
                                        Async Filter workers: 1
                                          filter: id=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_target
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: id=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t44\tX\n2\t0\tB\n3\t33\tZ\n");
                bindVariableService.clear();
            }
            dropRows();
        });
    }

    @Test
    public void testValidationErrorsPreserveDiagnosticsAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertError(compiler, "UPDATE uj_target t SET missing=s.v FROM uj_source s WHERE t.id=s.id", 23, "Invalid column: missing");
                assertError(compiler, "UPDATE uj_target t SET copied=s.missing FROM uj_source s WHERE t.id=s.id", 30, "Invalid column: s.missing");
                assertError(compiler, "UPDATE uj_target t SET ts=t.ts FROM uj_source s WHERE t.id=s.id", 23, "Designated timestamp column cannot be updated");
                assertError(compiler, "UPDATE uj_target t SET copied=s.v,COPIED=s.v FROM uj_source s WHERE t.id=s.id", 34, "Duplicate column [name=COPIED] in SET clause");
                assertError(compiler, "UPDATE uj_target t SET copied=s.v FROM uj_source s CROSS JOIN uj_target t2 WHERE t.id=s.id", 51, "JOIN is not supported on UPDATE statement");
                assertUpdate(compiler,
                        "UPDATE uj_target t SET copied=s.v FROM uj_source s WHERE t.id=s.id", 2,
                        """
                                SelectedRecord
                                    Hash Join Light
                                      condition: s.id=t.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: uj_target
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: uj_source
                                """);
                assertRows(compiler, "id\tcopied\tlabel\n1\t11\tA\n2\t0\tB\n3\t33\tC\n");
            }
        });
    }

    @Test
    public void testWalJoinsKeepExistingRejections() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE uj_wal(id INT,copied INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertError(compiler, "UPDATE uj_wal t SET copied=s.v FROM uj_source s WHERE t.id=s.id",
                        0, "UPDATE statements with join are not supported yet for WAL tables");
                assertError(compiler, "UPDATE uj_wal t SET copied=s.id FROM uj_wal s WHERE t.id=s.id",
                        0, "UPDATE statements with join are not supported yet for WAL tables");
            }
        });
    }

    private void assertError(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        try (UpdateOperation ignored = compiler.compile(sql, sqlExecutionContext).getUpdateOperation()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
        assertRows(compiler, "id\tcopied\tlabel\n1\t0\tA\n2\t0\tB\n3\t0\tC\n");
    }

    private void assertRows(SqlCompilerImpl compiler, String expected) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(
                "SELECT id,copied,label FROM uj_target ORDER BY id", sqlExecutionContext
        ).getRecordCursorFactory()) {
            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
        }
    }

    private void assertUpdate(SqlCompilerImpl compiler, String sql, long affectedRows, String expectedPlan) throws Exception {
        final CompiledQuery query = compiler.compile(sql, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, query.getType());
        try (UpdateOperation update = query.getUpdateOperation()) {
            Assert.assertEquals(engine.verifyTableName("uj_target").getTableId(), update.getTableId());
            Assert.assertTrue(update.getFactory().supportsUpdateRowId(engine.verifyTableName("uj_target")));
            TestUtils.assertEquals(sql, JitUtil.isJitSupported() ? expectedPlan : expectedPlan.replace("Async JIT", "Async"), planText(update.getFactory()));
            try (OperationFuture future = query.execute(null)) {
                future.await();
                Assert.assertEquals(sql, affectedRows, future.getAffectedRowsCount());
            }
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE uj_target(unused INT,id INT,copied INT,label SYMBOL,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO uj_target VALUES(91,1,0,'A','2020-01-01'),(92,2,0,'B','2020-01-02'),(93,3,0,'C','2020-01-03')");
        execute("CREATE TABLE uj_source(unused STRING,id INT,copied LONG,v INT,label SYMBOL)");
        execute("INSERT INTO uj_source VALUES('unused',1,101L,11,'X'),('unused',3,303L,33,'Z')");
    }

    private void dropRows() throws SqlException {
        execute("DROP TABLE uj_source");
        execute("DROP TABLE uj_target");
    }
}
