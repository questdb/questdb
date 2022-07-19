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

package io.questdb.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.WriteMode;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableWriteModeTest extends AbstractGriffinTest {

    @Test
    public void testDefaultWriteMode() throws Exception {
        assertMemoryLeak(() -> {
            defaultTableWriteMode = WriteMode.WAL;
            createTableWrite("my_table_wal", null, "HOUR");
            assertWriteMode("my_table_wal", WriteMode.WAL);

            defaultTableWriteMode = WriteMode.DIRECT;
            createTableWrite("my_table_dir", null, "HOUR");
            assertWriteMode("my_table_dir", WriteMode.DIRECT);
        });
    }

    @Test
    public void testWriteModeAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "ALTER COLUMN s ADD INDEX";
            checkWriteModeBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    public void testWriteModeAndAlterLag() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "set param commitLag=100s";
            checkWriteModeBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    public void testWriteModeAndRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            String alterSuffix = "rename column x to y";
            checkWriteModeBeforeAfterAlter(alterSuffix);
        });
    }

    @Test
    public void testWriteModeNameInCreateAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table wm as (" +
                    "select x, cast(x as timestamp) as ts " +
                    "from long_sequence(2) " +
                    ") timestamp(ts) partition by DAY with writeMode=WAL");

            assertWriteMode("wm", WriteMode.WAL);
        });
    }

    @Test
    public void testWriteModeNameInCreateAsSelect2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table wm as (" +
                    "select x, cast(x as timestamp) as ts " +
                    "from long_sequence(2) " +
                    ") timestamp(ts) partition by DAY with writeMode=DIREcT");

            assertWriteMode("wm", WriteMode.DIRECT);
        });
    }

    @Test
    public void testWriteModeNameInvalid() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "NONE", "DAY");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "unrecognized Write Mode 'NONE'"
                );
            }
        });
    }

    @Test
    public void testWriteModeNameInvalidEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "", "DAY");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "too few arguments for '='"
                );
            }
        });
    }

    @Test
    public void testWriteModeNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            try {
                createTableWrite("my_table_wal", "WAL", "NONE");
                Assert.fail("Exception expected");
            } catch (SqlException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "WAL Write Mode can only be used on partitioned tables"
                );
            }
        });
    }

    private void assertWriteMode(String tableName, int writeMode) {
        try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            Assert.assertEquals(writeMode, rdr.getMetadata().getWriteMode());
        }
    }

    private void checkWriteModeBeforeAfterAlter(String alterSuffix) throws SqlException {
        createTableWrite("my_table_wal", "WAL", "DAY");
        assertWriteMode("my_table_wal", WriteMode.WAL);
        compile("alter table my_table_wal " + alterSuffix, sqlExecutionContext);
        assertWriteMode("my_table_wal", WriteMode.WAL);

        createTableWrite("my_table_dir", "DIRECT", "DAY");
        assertWriteMode("my_table_dir", WriteMode.DIRECT);
        compile("alter table my_table_dir " + alterSuffix, sqlExecutionContext);
        assertWriteMode("my_table_dir", WriteMode.DIRECT);

        assertSql("select name, writeMode from tables() order by name",
                "name\twriteMode\n" +
                "my_table_dir\tDirect\n" +
                "my_table_wal\tWAL\n");
    }

    private void createTableWrite(String tableName, String writeMode, String partitionBY) throws SqlException {
        compile(
                "create table " + tableName +
                        " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                        " PARTITION BY " + partitionBY +
                        (writeMode != null ? " WITH WriteMode=" + writeMode : "")
        );
    }
}
