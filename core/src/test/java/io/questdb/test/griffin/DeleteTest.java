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

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DeleteTest extends AbstractCairoTest {

    @Test
    public void testDeleteCompilesToDeleteType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cc = compiler.compile("DELETE FROM t WHERE x = 1", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.DELETE, cc.getType());
                Assert.assertNotNull(cc.getDeleteOperation());
            }
        });
    }

    @Test
    public void testDeleteRequiresWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WHERE");
            }
        });
    }

    @Test
    public void testDeleteRejectsNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY BYPASS WAL");
            try {
                execute("DELETE FROM t WHERE x = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WAL");
            }
        });
    }

    @Test
    public void testDeleteRejectsPlainView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            execute("create view t_view as (select ts, max(x) as x from t sample by 1h)");
            try {
                execute("DELETE FROM t_view WHERE x = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "view");
            }
        });
    }

    @Test
    public void testDeleteRejectsUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t WHERE nope = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "nope");
            }
        });
    }
}
