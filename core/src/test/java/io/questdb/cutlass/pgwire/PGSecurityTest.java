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

package io.questdb.cutlass.pgwire;

import io.questdb.std.Os;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.fail;

public class PGSecurityTest extends BasePGTest {

    @ClassRule
    public static TemporaryFolder backup = new TemporaryFolder();

    private static final PGWireConfiguration READ_ONLY_CONF = new DefaultPGWireConfiguration() {
        @Override
        public boolean readOnlySecurityContext() {
            return true;
        }
    };

    @Test
    public void testDisallowDrop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDisallowed("drop table src");
        });
    }

    @Test
    public void testDisallowAddNewColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDisallowed("alter table src add column newCol string");
        });
    }

    @Test
    public void testDisallowDelete() throws Exception {
        // we don't support DELETE yet. this test exists as a reminder to check read-only security context is honoured
        // when/if DELETE is implemented.
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            try {
                executeWithPg("delete from src");
                fail("It appears delete are implemented. Please change this test to check DELETE are refused with the read-only context");
            } catch (PSQLException e) {
                // the parser does not support DELETE
                assertContains(e.getMessage(), "unexpected token: from");
            }
        });
    }

    @Test
    public void testDisallowCreateTable() throws Exception {
        assertMemoryLeak(() -> assertQueryDisallowed("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY"));
    }

    @Test
    public void testDisallowInsert() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            assertQueryDisallowed("insert into src values (now(), 'foo')");
        });
    }

    @Test
    public void testDisallowUpdate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            executeInsert("insert into src values ('2022-04-12T17:30:45.145921Z', 'foo')");

            try {
                executeWithPg("update src set name = 'bar'");
            } catch (PSQLException e) {
                // the parser does not support DELETE
                assertContains(e.getMessage(), "Write permission denied");
            }

            // if this asserts fails then it means UPDATE are already implemented
            // please change this test to check the update throws an exception in the read-only mode
            // this is in place so we won't forget to test UPDATE honours read-only security context.
            assertSql("select * from src", "ts\tname\n" +
                    "2022-04-12T17:30:45.145921Z\tfoo\n");
        });
    }

    @Test
    public void testDisallowInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("insert into src select now(), name from src");
        });
    }

    @Test
    public void testDisallowVacuum() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            assertQueryDisallowed("vacuum partitions src");
        });
    }

    @Test
    public void testDisallowTruncate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("truncate table src");
        });
    }

    @Test
    public void testDisallowSnapshotComplete() throws Exception {
        // snapshot is not supported on windows at all
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("snapshot prepare", sqlExecutionContext);
            try {
                assertQueryDisallowed("snapshot complete");
            } finally {
                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testDisallowSnapshotPrepare() throws Exception {
        // snapshot is not supported on windows at all
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            assertQueryDisallowed("snapshot prepare");
        });
    }

    @Test
    public void testDisallowsBackupDatabase() throws Exception {
        assertMemoryLeak(() -> {
            configureForBackups();
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("backup database");
        });
    }

    @Test
    public void testDisallowsBackupTable() throws Exception {
        assertMemoryLeak(() -> {
            configureForBackups();
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("backup table src");
        });
    }

    @Test
    @Ignore("This is failing, but repair is nop so that's ok")
    public void testDisallowsRepairTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("repair table src");
        });
    }

    @Test
    public void testAllowsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            executeWithPg("select * from src");
        });
    }

    private void assertQueryDisallowed(String query) throws Exception {
        try {
            executeWithPg(query);
            fail("Query '" + query + "' must fail in the read-only mode!");
         } catch (PSQLException e) {
            assertContains(e.getMessage(), "Write permission denied");
        }
    }

    private void executeWithPg(String query) throws Exception {
        try (
                final PGWireServer ignored = createPGServer(READ_ONLY_CONF);
                final Connection connection = getConnection(false, true);
                final Statement statement = connection.createStatement()
        ) {
            statement.execute(query);
        }
    }
}
