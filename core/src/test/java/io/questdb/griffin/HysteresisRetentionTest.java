/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class HysteresisRetentionTest extends AbstractGriffinTest {

    @Test
    public void testRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            compiler.compile("alter table my_table rename column x to y", sqlExecutionContext);
            assertHysteresis();
        });
    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            compiler.compile("alter table my_table add column y symbol", sqlExecutionContext);
            assertHysteresis();
        });
    }

    @Test
    public void testDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            compiler.compile("alter table my_table drop column x", sqlExecutionContext);
            assertHysteresis();
        });
    }

    @Test
    public void testAddIndexToEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            compiler.compile("ALTER TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertHysteresis();
        });
    }

    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            executeInsert("insert into my_table values(0, 1000, 'a')");
            compiler.compile("ALTER TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertHysteresis();
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertHysteresis();
            executeInsert("insert into my_table values(to_timestamp('1970-01-01', 'yyyy-dd-MM'), 2000, 'a')");
            executeInsert("insert into my_table values(to_timestamp('1970-01-02', 'yyyy-dd-MM'), 2000, 'a')");
            assertHysteresis();
            compiler.compile("ALTER TABLE my_table DROP PARTITION LIST '1970-01-01'", sqlExecutionContext);
            assertHysteresis();
        });
    }


    private void assertHysteresis() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "tables()",
                sink,
                "id\tname\tdesignatedTimestamp\tpartitionBy\to3MaxUncommittedRows\to3CommitHysteresisMicros\n" +
                        "1\tmy_table\ttimestamp\tDAY\t250000\t240000000\n"
        );
    }

    private void createTable() throws SqlException {
        compiler.compile("CREATE TABLE my_table (timestamp TIMESTAMP, x long, s symbol) timestamp(timestamp)\n" +
                "PARTITION BY DAY WITH o3MaxUncommittedRows=250000, o3CommitHysteresis=240s", sqlExecutionContext);
    }
}
