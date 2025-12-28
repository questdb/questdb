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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TablesFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testMetadataQuery() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            2\ttable2\tts2\tNONE\t1000\t300000000
                            1\ttable1\tts1\tDAY\t1000\t300000000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() order by id desc"
            );
        });
    }

    @Test
    public void testMetadataQueryDefaultHysteresisParams() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 83737);
            node1.setProperty(PropertyKey.CAIRO_O3_MAX_LAG, 28);

            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            1\ttable1\tts1\tDAY\t83737\t28000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
            );
        });
    }

    @Test
    public void testMetadataQueryMissingMetaFile() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            FilesFacade filesFacade = configuration.getFilesFacade();
            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName("table1");
                path.concat(configuration.getDbRoot()).concat(tableToken).concat(META_FILE_NAME).$();
                filesFacade.remove(path.$());
            }

            assertException(
                    "select hydrate_table_metadata('*')",
                    7,
                    "could not open, file does not exist"
            );
        });
    }

    @Test
    public void testMetadataQueryWithWhere() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag
                            1\ttable1\tts1\tDAY\t1000\t300000000
                            """,
                    "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'table1'"
            );
        });
    }

    @Test
    public void testMetadataQueryWithWhereAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY);
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
            tm1 = new TableModel(configuration, "table2", PartitionBy.NONE);
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);

            assertSql(
                    """
                            designatedTimestamp
                            ts1
                            """,
                    "select designatedTimestamp from tables where table_name = 'table1'"
            );
        });
    }

    @Test
    public void testRowCountAndLastWriteTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create a WAL table and write data to it
            execute("CREATE TABLE test_writes (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            RecentWriteTracker tracker = engine.getRecentWriteTracker();
            tracker.clear();

            // Before any writes, rowCount and lastWriteTimestamp should be null
            assertSql(
                    """
                            table_name\trowCount\tlastWriteTimestamp
                            test_writes\tnull\t
                            """,
                    "select table_name, rowCount, lastWriteTimestamp from tables() where table_name = 'test_writes'"
            );

            // Insert rows and drain WAL
            long beforeWrite = configuration.getMicrosecondClock().getTicks();
            execute("INSERT INTO test_writes VALUES (now(), 1)");
            execute("INSERT INTO test_writes VALUES (now(), 2)");
            execute("INSERT INTO test_writes VALUES (now(), 3)");
            drainWalQueue();
            long afterWrite = configuration.getMicrosecondClock().getTicks();

            // Verify row count is tracked
            TableToken tableToken = engine.verifyTableName("test_writes");
            Assert.assertEquals(3L, tracker.getRowCount(tableToken));

            // Query via tables() function
            assertSql(
                    """
                            table_name\trowCount
                            test_writes\t3
                            """,
                    "select table_name, rowCount from tables() where table_name = 'test_writes'"
            );

            // Verify lastWriteTimestamp is within expected range
            long lastWriteTimestamp = tracker.getWriteTimestamp(tableToken);
            Assert.assertTrue("Timestamp should be >= beforeWrite", lastWriteTimestamp >= beforeWrite);
            Assert.assertTrue("Timestamp should be <= afterWrite", lastWriteTimestamp <= afterWrite);
        });
    }
}
