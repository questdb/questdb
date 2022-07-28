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

import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class CommitLagRetentionTest extends AbstractGriffinTest {

    @Test
    public void testRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            compile("alter table my_table rename column x to y", sqlExecutionContext);
            assertCommitLagValues();
        });
    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            compile("alter table my_table add column y symbol", sqlExecutionContext);
            assertCommitLagValues();
        });
    }

    @Test
    public void testDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            compile("alter table my_table drop column x", sqlExecutionContext);
            assertCommitLagValues();
        });
    }

    @Test
    public void testAddIndexToEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            compile("alter TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertCommitLagValues();
        });
    }

    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            executeInsert("insert into my_table values(0, 1000, 'a')");
            compile("alter TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertCommitLagValues();
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertCommitLagValues();
            executeInsert("insert into my_table values(to_timestamp('1970-01-01', 'yyyy-dd-MM'), 2000, 'a')");
            executeInsert("insert into my_table values(to_timestamp('1970-01-02', 'yyyy-dd-MM'), 2000, 'a')");
            assertCommitLagValues();
            compile("alter TABLE my_table DROP PARTITION LIST '1970-01-01'", sqlExecutionContext);
            assertCommitLagValues();
        });
    }


    private void assertCommitLagValues() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,commitLag from tables()",
                sink,
                "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                        "1\tmy_table\ttimestamp\tDAY\t250000\t240000000\n"
        );
    }

    private void createTable() throws SqlException {
        compiler.compile("CREATE TABLE my_table (timestamp TIMESTAMP, x long, s symbol) timestamp(timestamp)\n" +
                "PARTITION BY DAY WITH maxUncommittedRows=250000, commitLag=240s", sqlExecutionContext);
    }
}
