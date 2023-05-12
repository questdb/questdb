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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.test.cairo.TableModel;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TableListFunctionFactoryTest extends AbstractGriffinTest {
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
                "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() order by id desc",
                "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "2\ttable2\tts2\tNONE\t1000\t300000000\n" +
                        "1\ttable1\tts1\tDAY\t1000\t300000000\n"
        );
    }

    @Test
    public void testMetadataQueryDefaultHysteresisParams() throws Exception {
        configOverrideMaxUncommittedRows(83737);
        configOverrideO3MaxLag();

        try (TableModel tm1 = new TableModel(configuration, "table1", PartitionBy.DAY)) {
            tm1.col("abc", ColumnType.STRING);
            tm1.timestamp("ts1");
            createPopulateTable(tm1, 0, "2020-01-01", 0);
        }

        assertSql(
                "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()",
                "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "1\ttable1\tts1\tDAY\t83737\t28291\n"
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
                "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()",
                "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "2\ttable2\tts2\tNONE\t1000\t300000000\n"
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
                "select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() where name = 'table1'",
                "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                        "1\ttable1\tts1\tDAY\t1000\t300000000\n"
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
                "select designatedTimestamp from tables where name = 'table1'",
                "designatedTimestamp\n" +
                        "ts1\n"
        );
    }
}
