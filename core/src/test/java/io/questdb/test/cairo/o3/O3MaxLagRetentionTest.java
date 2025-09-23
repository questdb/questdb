/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.o3;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class O3MaxLagRetentionTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public O3MaxLagRetentionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("alter table my_table add column y symbol", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("insert into my_table values(0, 1000, 'a')");
            execute("alter TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    @Test
    public void testAddIndexToEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("alter TABLE my_table ALTER COLUMN s ADD INDEX", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    @Test
    public void testDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("alter table my_table drop column x", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("insert into my_table values(to_timestamp('1970-01-01', 'yyyy-dd-MM'), 2000, 'a')");
            execute("insert into my_table values(to_timestamp('1970-01-02', 'yyyy-dd-MM'), 2000, 'a')");
            assertO3MaxLagValues();
            execute("alter TABLE my_table DROP PARTITION LIST '1970-01-01'", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    @Test
    public void testRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertO3MaxLagValues();
            execute("alter table my_table rename column x to y", sqlExecutionContext);
            assertO3MaxLagValues();
        });
    }

    private void assertO3MaxLagValues() throws SqlException {
        TestUtils.assertSql(
                engine,
                sqlExecutionContext,
                "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()",
                sink,
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "1\tmy_table\ttimestamp\tDAY\t250000\t240000000\n"
        );
    }

    private void createTable() throws SqlException {
        executeWithRewriteTimestamp("CREATE TABLE my_table (timestamp #TIMESTAMP, x long, s symbol) timestamp(timestamp)\n" +
                "PARTITION BY DAY WITH maxUncommittedRows=250000, o3MaxLag=240s", timestampType.getTypeName());
    }
}
