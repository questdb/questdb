/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class ShowTablesFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testMetadataQuery() throws Exception {
        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }
        try (TableModel tm1 = new TableModel(configuration, "table2", PartitionBy.NONE)) {
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        assertSql(
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "2\ttable2\tts2\tNONE\t1000\t300000000\n" +
                        "1\ttable1\tts1\tDAY\t1000\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() order by id desc"
        );
    }

    @Test
    public void testMetadataQueryDefaultHysteresisParams() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 83737);
        node1.setProperty(PropertyKey.CAIRO_O3_MAX_LAG, 28);

        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        assertSql(
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "1\ttable1\tts1\tDAY\t83737\t28000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
        );
    }

    @Test
    public void testMetadataQueryMissingMetaFile() throws Exception {
        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }
        try (TableModel tm1 = new TableModel(configuration, "table2", PartitionBy.NONE)) {
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        engine.releaseAllWriters();
        engine.releaseAllReaders();

        FilesFacade filesFacade = configuration.getFilesFacade();
        try (Path path = new Path()) {
            TableToken tableToken = engine.verifyTableName("table1");
            path.concat(configuration.getRoot()).concat(tableToken).concat(META_FILE_NAME).$();
            filesFacade.remove(path);
        }

        refreshTablesInBaseEngine();
        assertSql(
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "2\ttable2\tts2\tNONE\t1000\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
        );
    }

    @Test
    public void testMetadataQueryWithWhere() throws Exception {
        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }
        try (TableModel tm1 = new TableModel(configuration, "table2", PartitionBy.NONE)) {
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        assertSql(
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "1\ttable1\tts1\tDAY\t1000\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where table_name = 'table1'"
        );
    }

    @Test
    public void testMetadataQueryWithWhereAndSelect() throws Exception {
        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }
        try (TableModel tm1 = new TableModel(configuration, "table2", PartitionBy.NONE)) {
            tm1.timestamp("ts2");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        assertSql(
                "designatedTimestamp\n" +
                        "ts1\n", "select designatedTimestamp from tables where table_name = 'table1'"
        );
    }
}
