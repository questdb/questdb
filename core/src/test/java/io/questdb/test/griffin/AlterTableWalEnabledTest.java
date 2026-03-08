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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AlterTableWalEnabledTest extends AbstractCairoTest {


    @Test
    public void testDefaultWalEnabledMode() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            createTableWrite("my_table_wal", null, "HOUR");
            assertWalEnabled("my_table_wal", true);

            createTableWrite("my_table_wal_none", null, "NONE");
            assertWalEnabled("my_table_wal_none", false);

            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, false);
            createTableWrite("my_table_dir", null, "HOUR");
            assertWalEnabled("my_table_dir", false);
        });
    }

    @Test
    public void testInvalidWalWord() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "BYPASS WALL", "DAY");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "invalid syntax, should be BYPASS WAL but was BYPASS WALL"
                );
                Assert.assertEquals(
                        "create table my_table_wal (ts TIMESTAMP, x long, s symbol) timestamp(ts) PARTITION BY DAY BYPASS ".length(),
                        ex.getPosition()
                );
            }
        });
    }

    @Test
    @Ignore
    public void testWalEnabledAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "ALTER COLUMN s ADD INDEX";
            checkWalEnabledBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    @Ignore
    public void testWalEnabledAndAlterLag() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "set param o3MaxLag=100s";
            checkWalEnabledBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    public void testWalEnabledAndRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "rename column x to y";
            checkWalEnabledBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    public void testWalEnabledMissingWalKeyword() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "BYPASS", "DAY");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "invalid syntax, should be BYPASS WAL but was BYPASS"
                );
                Assert.assertEquals(
                        "create table my_table_wal (ts TIMESTAMP, x long, s symbol) timestamp(ts) PARTITION BY DAY BYPASS".length(),
                        ex.getPosition()
                );
            }
        });
    }

    @Test
    public void testWalEnabledNameInCreateAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wm as (" +
                    "select x, cast(x as timestamp) as ts " +
                    "from long_sequence(2) " +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalEnabled("wm", true);
        });
    }

    @Test
    public void testWalEnabledNameInCreateAsSelect2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wm as (" +
                    "select x, cast(x as timestamp) as ts " +
                    "from long_sequence(2) " +
                    ") timestamp(ts) partition by DAY Bypass WaL");

            assertWalEnabled("wm", false);
        });
    }

    @Test
    public void testWalEnabledNameInvalid() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "NONE", "DAY");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "unexpected token [NONE]"
                );
                Assert.assertEquals(
                        "create table my_table_wal (ts TIMESTAMP, x long, s symbol) timestamp(ts) PARTITION BY DAY ".length(),
                        ex.getPosition()
                );
            }
        });
    }

    @Test
    public void testWalEnabledNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "WAL", "NONE");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "WAL Write Mode can only be used on partitioned tables"
                );
                Assert.assertEquals(
                        "create table my_table_wal (ts TIMESTAMP, x long, s symbol) timestamp(ts) PARTITION BY NONE ".length(),
                        ex.getPosition()
                );
            }
        });
    }

    private void assertWalEnabled(String tableName, boolean enabled) {
        try (TableReader rdr = engine.getReader(tableName)) {
            Assert.assertEquals(enabled, rdr.getMetadata().isWalEnabled());
        }
    }

    private void checkWalEnabledBeforeAfterAlter(String alterSuffix) throws SqlException {
        createTableWrite("my_table_wal", "WAL", "DAY");
        assertWalEnabled("my_table_wal", true);
        execute("alter table my_table_wal " + alterSuffix, sqlExecutionContext);
        assertWalEnabled("my_table_wal", true);

        createTableWrite("my_table_dir", "BYPASS WAL", "DAY");
        assertWalEnabled("my_table_dir", false);
        execute("alter table my_table_dir " + alterSuffix, sqlExecutionContext);
        assertWalEnabled("my_table_dir", false);

        assertSql("table_name\twalEnabled\n" +
                "my_table_dir\tfalse\n" +
                "my_table_wal\ttrue\n", "select table_name, walEnabled from tables() order by table_name"
        );
    }

    private void createTableWrite(String tableName, String walMode, String partitionBY) throws SqlException {
        execute(
                "create table " + tableName +
                        " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                        " PARTITION BY " + partitionBY +
                        (walMode != null ? " " + walMode : "")
        );
    }
}
