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

import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.fail;

public class PGSecurityTest extends BasePGTest {

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
    @Ignore("DELETE not implemented yet")
    public void testDisallowDelete() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDisallowed("delete from src");
        });
    }

    @Test
    @Ignore("UPDATE security not implemented yet")
    public void testDisallowUpdate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            compiler.compile("insert into src values (now(), 'foo')", sqlExecutionContext);
            assertQueryDisallowed("update src set name = 'bar'");
        });
    }

    @Test
    public void testAllowsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDoesNotThrowException("select * from src");
        });
    }

    private void assertQueryDisallowed(String query) throws Exception {
        try (
                final PGWireServer ignored = createPGServer(READ_ONLY_CONF);
                final Connection connection = getConnection(false, true);
                final PreparedStatement statement = connection.prepareStatement(query)
        ) {
            statement.execute();
            fail("Query '" + query + "' must fail in the read-only mode!");
        } catch (PSQLException e) {
            assertContains(e.getMessage(), "Write permission denied");
        }
    }

    private void assertQueryDoesNotThrowException(String query) throws Exception {
        try (
                final PGWireServer ignored = createPGServer(READ_ONLY_CONF);
                final Connection connection = getConnection(false, true);
                final PreparedStatement statement = connection.prepareStatement(query)
        ) {
            statement.execute();
        }
    }
}
