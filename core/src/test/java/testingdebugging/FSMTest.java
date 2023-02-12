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

package testingdebugging;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static io.questdb.griffin.CompiledQuery.TRUNCATE;

public class FSMTest extends AbstractGriffinTest {

    // TODO our tests of Insert Data
    // state transition 1 : empty table -> operation: insert data -> non-empty table
    // state transition 2 : non-empty table -> operation: insert data -> non-empty table

    public void createTab() throws SqlException {
        compiler.compile("create table tab (id int, text string)", sqlExecutionContext);
    }

    @Test
    public void testQuestDBStateMachineAtEmptyTableState() throws Exception {
        assertMemoryLeak(() -> {
                    createTab();
                    assertSql("'tab'", "id\ttext\n");
                    assertQuery(
                            "count\n" +
                                    "0\n",
                            "select count() from tab",
                            null,
                            false,
                            true
                    );
                }
        );
    }

    @Test
    public void testQuestDBStateMachineAtNonEmptyTableStateInsert() throws Exception {
        assertMemoryLeak(
                () -> {
                    createTab();
                    executeInsert("insert into tab values (1, 'test')");
                    assertSql("'tab'", "id\ttext\n" +
                            "1\ttest\n");
                }
        );
    }

    // TODO our tests of Update Data
    // state transition 1 : empty table -> operation: update data -> empty table
    // state transition 2 : non-empty table -> operation: update data -> non-empty table

    private void executeUpdate(String query) throws SqlException {
        executeOperation(query, CompiledQuery.UPDATE);
    }

    @Test
    public void testQuestDBStateMachineAtEmptyTableStateUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createTab();
            executeUpdate("update tab set text = 'test2' where text = 'test'");
            assertSql("'tab'", "id\ttext\n");
            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testQuestDBStateMachineAtNonEmptyTableStateUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createTab();
            executeInsert("insert into tab values (1, 'test');");
            executeUpdate("update tab set text = 'test2' where text = 'test'");
            assertSql("'tab'", "id\ttext\n" +
                    "1\ttest2\n");
        });
    }

    // TODO our tests of Truncate Table
    // state transition 1 : empty table -> operation: truncate -> empty table
    // state transition 2 : non-empty table -> operation: truncate -> empty table

    @Test
    public void testQuestDBStateMachineAtEmptyTableStateTruncate() throws Exception {
        assertMemoryLeak(() -> {
            createTab();
            Assert.assertEquals(TRUNCATE, compiler.compile("truncate table tab;", sqlExecutionContext).getType());
            assertSql("'tab'", "id\ttext\n");
            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from tab",
                    null,
                    false,
                    true
            );
        });
    }


    @Test
    public void testQuestDBStateMachineAtNonEmptyTableStateTruncate() throws Exception {
        assertMemoryLeak(() -> {
            createTab();
            executeInsert("insert into tab values (1, 'test')");
            Assert.assertEquals(TRUNCATE, compiler.compile("truncate table tab;", sqlExecutionContext).getType());
            assertSql("'tab'", "id\ttext\n");
            assertQuery(
                    "count\n" +
                            "0\n",
                    "select count() from tab",
                    null,
                    false,
                    true
            );
        });
    }

}
