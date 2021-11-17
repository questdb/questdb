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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import org.junit.Test;

public class SqlParserUpdateTest extends AbstractSqlParserTest {
    @Test
    public void testSelectSingleTableWithJoinInFrom() throws Exception {
        assertQuery("select-virtual rowid() rowid, 1 tt from (select [x] from tblx x join select [y] from tbly y on y.y = x.x where x > 10) x",
                "select rowid(), 1 as tt from tblx x, tbly y where x.x = y.y and x > 10",
                partitionedModelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateNonPartitionedTableFails() throws Exception {
        assertSyntaxError(
                "update tblx set x = 1",
                0,
                "UPDATE query can only be executed on partitioned tables",
                modelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
        );
    }

    @Test
    public void testUpdateSingleTableToConst() throws Exception {
        assertUpdate("update x set tt = 1 from (x timestamp (timestamp))",
                "update x set tt = 1",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp());
    }

    @Test
    public void testUpdateSingleTableWithAlias() throws Exception {
        assertUpdate("update tblx as x set tt = tt + 1 from (tblx x timestamp (timestamp) where t = NULL)",
                "update tblx x set tt = tt + 1 WHERE x.t = NULL",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp());
    }

    @Test
    public void testUpdateSingleTableWithJoinInFrom() throws Exception {
        assertUpdate("update tblx set tt = 1 from (tblx timestamp (timestamp) join tbly y on y = x where x > 10)",
                "update tblx set tt = 1 from tbly y where x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateSingleTableWithWhere() throws Exception {
        assertUpdate("update x set tt = t from (x timestamp (timestamp) where t > '2005-04-02T12:00:00')",
                "update x set tt = t where t > '2005-04-02T12:00:00'",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateDesignatedTimestampFails() throws Exception {
        assertSyntaxError("update x set tt = 1",
                13,
                "Designated timestamp column cannot be updated",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .timestamp("tt"));
    }

    @Test
    public void testUpdateWithAggregatesFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = count() where x > 10",
                "update tblx as xx set tt = ".length(),
                "Unsupported function in SET clause",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithInvalidColumnInSetLeftFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set invalidcol = t where x > 10",
                "update tblx as xx set ".length(),
                "Invalid column: invalidcol",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithInvalidColumnInSetRightFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set t = invalidcol where x > 10",
                "update tblx as xx set t = ".length(),
                "Invalid column: invalidcol",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithInvalidColumnInWhereFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = t + 1 where invalidcol > 10",
                "update tblx as xx set tt = t + 1 where ".length(),
                "Invalid column: invalidcol",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithJoinAndTableAlias() throws Exception {
        assertUpdate("update tblx as xx set tt = 1 from (tblx xx timestamp (timestamp) join tbly y on y = xx.x where x > 10)",
                "update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateWithLatestByFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = 1 where x > 10 LATEST BY s",
                "update tblx as xx set tt = 1 where x > 10 ".length(),
                "unexpected token: LATEST",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithLimitFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10 LIMIT 10",
                "update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10 ".length(),
                "unexpected token: LIMIT",
                partitionedModelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateWithLimitInJoin() throws Exception {
        assertUpdate("update tblx as xx set tt = 1 from (tblx xx timestamp (timestamp) join ((tbly) limit 10) y on y = xx.x where x > 10)",
                "update tblx as xx set tt = 1 from (tbly LIMIT 10) y where xx.x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateWithSampleByFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = 1 where x > 10 SAMPLE BY 1h",
                "update tblx as xx set tt = 1 where x > 10 ".length(),
                "unexpected token: SAMPLE",
                partitionedModelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    private static TableModel partitionedModelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.DAY);
    }
}
