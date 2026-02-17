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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.test.cairo.TableModel;
import org.junit.Test;

public class SqlParserUpdateTest extends AbstractSqlParserTest {
    @Test
    public void testUpdateAmbiguousColumnFails() throws Exception {
        assertSyntaxError(
                "update tblx set y = y from tbly y where tblx.x = tbly.y and tblx.x > 10",
                "update tblx set y = ".length(),
                "Ambiguous column [name=y]",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("y", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateDesignatedTimestampFails() throws Exception {
        assertSyntaxError(
                "update x set tt = 1",
                13,
                "Designated timestamp column cannot be updated",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .timestamp("tt")
        );
    }

    @Test
    public void testUpdateEmptyWhereFails() throws Exception {
        assertSyntaxError(
                "update tblx set tt = 1 where ",
                "update tblx set tt = 1 where".length(),
                "empty where clause",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateJoinInvalidSyntaxFails() throws Exception {
        assertSyntaxError(
                "update tblx set tt = 1 join tblx on x = y and x > 10",
                "update tblx set tt = 1 ".length(),
                "FROM, WHERE or EOF expected",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateJoinTableWithDoubleFromFails() throws Exception {
        assertSyntaxError(
                "update tblx set tt = 1 from tblx from tbly where x = y and x > 10",
                "update tblx set tt = 1 from tblx ".length(),
                "unexpected token [from]",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateNoSetFails() throws Exception {
        assertSyntaxError(
                "update tblx x = 1",
                "update tblx x ".length(),
                "SET expected",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp("tt")
        );
    }

    @Test
    public void testUpdateSameColumnTwiceFails0() throws Exception {
        assertSyntaxError(
                "update tblx set x = 1, s = 'abc', x = 2",
                "update tblx set x = 1, s = 'abc', ".length(),
                "Duplicate column [name=x] in SET clause",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSameColumnTwiceFails1() throws Exception {
        assertSyntaxError(
                "update tblx set x = 1, s = 'abc', \"X\" = 2",
                "update tblx set x = 1, s = 'abc', ".length(),
                "Duplicate column [name=X] in SET clause",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSameColumnTwiceFailsNonAscii() throws Exception {
        assertSyntaxError(
                "update tblx set 侘寂 = 1, s = 'abc', 侘寂 = 2",
                35,
                "Duplicate column [name=侘寂] in SET clause",
                partitionedModelOf("tblx")
                        .col("侘寂", ColumnType.TIMESTAMP)
                        .col("s", ColumnType.SYMBOL)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSingleTableEndsSemicolon() throws Exception {
        assertUpdate(
                "update tblx set tt = tt + 1 from (select-virtual tt + 1 tt from (select [tt, t] from tblx timestamp (timestamp) where t = NULL))",
                "update tblx set tt = tt + 1 WHERE t = NULL;",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSingleTableToBindVariable() throws Exception {
        assertUpdate(
                "update x set tt = $1 from (select-virtual $1 tt from (x timestamp (timestamp)))",
                "update x set tt = $1",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSingleTableToConst() throws Exception {
        assertUpdate(
                "update x set tt = 1 from (select-virtual 1 tt from (x timestamp (timestamp)))",
                "update x set tt = 1",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSingleTableWithAlias() throws Exception {
        assertUpdate(
                "update tblx as x set tt = tt + 1 from (select-virtual tt + 1 tt from (select [tt, t] from tblx x timestamp (timestamp) where t = NULL))",
                "update tblx x set tt = tt + 1 WHERE x.t = NULL",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateSingleTableWithJoinAndConstFiltering() throws Exception {
        assertUpdate(
                "update tblx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx timestamp (timestamp) join select [y] from tbly y on y = x where x > 10 const-where 100 > 100))",
                "update tblx set tt = 1 from tbly y where x = y and x > 10 and 100 > 100",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateSingleTableWithJoinAndFiltering() throws Exception {
        assertUpdate(
                "update tblx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx timestamp (timestamp) join (select [y, t] from tbly y where t > 100) y on y = x where x > 10))",
                "update tblx set tt = 1 from tbly y where x = y and x > 10 and y.t > 100",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateSingleTableWithJoinAndNestedSampleBy() throws Exception {
        assertUpdate(
                "update tblx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx timestamp (timestamp) join select [y] from (select-group-by [first(y) y, ts] ts, first(y) y from (select [y, ts] from tbly timestamp (ts)) sample by 1h) y on y = x))",
                "update tblx set tt = 1 from (select ts, first(y) as y from tbly SAMPLE BY 1h ALIGN TO FIRST OBSERVATION) y where x = y",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.INT)
                        .timestamp("ts")
        );

        assertUpdate(
                "update tblx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx timestamp (timestamp) join select [y] from (select-group-by [first(y) y, timestamp_floor('1h', ts, null, '00:00', null) ts] timestamp_floor('1h', ts, null, '00:00', null) ts, first(y) y from (select [y, ts] from tbly timestamp (ts) stride 1h) order by ts) y on y = x))",
                "update tblx set tt = 1 from (select ts, first(y) as y from tbly SAMPLE BY 1h ALIGN TO CALENDAR) y where x = y",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("y", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    @Test
    public void testUpdateSingleTableWithJoinInFrom() throws Exception {
        assertUpdate(
                "update tblx set tt = tt + 1 from (select-virtual tt + 1 tt from (select [tt, x] from tblx timestamp (timestamp) join select [y] from tbly y on y = x where x > 10))",
                "update tblx set tt = tt + 1 from tbly y where x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateSingleTableWithWhere() throws Exception {
        assertUpdate(
                "update x set tt = t from (select-choose t tt from (select [t] from x timestamp (timestamp) where t > '2005-04-02T12:00:00'))",
                "update x set tt = t where t > '2005-04-02T12:00:00'",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateTwoColumnsToConst() throws Exception {
        assertUpdate(
                "update x set tt = 1,x = 2 from (select-virtual 1 tt, 2 x from (x timestamp (timestamp)))",
                "update x set tt = 1, x = 2",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .timestamp()
        );
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
    public void testUpdateWithCrossJoinAndSemicolon() throws Exception {
        String expected = "update tblx set tt = 1 from (select-virtual 1 tt from (tblx timestamp (timestamp) cross join tbly y))";
        assertUpdate(
                expected,
                "update tblx set tt = 1 from tbly y",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );

        assertUpdate(
                expected,
                "update tblx set tt = 1 from tbly y;",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
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
        assertUpdate(
                "update tblx as xx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx xx timestamp (timestamp) join select [y] from tbly y on y = xx.x where x > 10))",
                "update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateWithJoinKeywordFails() throws Exception {
        assertSyntaxError(
                "update tblx set tt = 1 from tblx join tbly where x = y and x > 10",
                "update tblx set tt = 1 from tblx ".length(),
                "JOIN is not supported on UPDATE statement",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateWithLatestByFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = 1 where x > 10 LATEST BY s",
                "update tblx as xx set tt = 1 where x > 10 ".length(),
                "unexpected token [LATEST]",
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
                "unexpected token [LIMIT]",
                partitionedModelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateWithLimitInJoin() throws Exception {
        assertUpdate(
                "update tblx as xx set tt = 1 from (select-virtual 1 tt from (select [x] from tblx xx timestamp (timestamp) join select [y] from (select-choose [y] t, y from (select [y] from tbly) limit 10) y on y = xx.x where x > 10))",
                "update tblx as xx set tt = 1 from (tbly LIMIT 10) y where xx.x = y and x > 10",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("x", ColumnType.INT)
                        .col("tt", ColumnType.INT)
                        .timestamp(),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateWithSampleByFails() throws Exception {
        assertSyntaxError(
                "update tblx as xx set tt = 1 where x > 10 SAMPLE BY 1h",
                "update tblx as xx set tt = 1 where x > 10 ".length(),
                "unexpected token [SAMPLE]",
                partitionedModelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                partitionedModelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT)
        );
    }

    @Test
    public void testUpdateWithSemicolon() throws Exception {
        assertUpdate(
                "update x set tt = 1 from (select-virtual 1 tt from (x timestamp (timestamp)))",
                "update x set tt = 1;",
                partitionedModelOf("x")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }

    @Test
    public void testUpdateWithWhereAndSemicolon() throws Exception {
        assertUpdate(
                "update tblx as x set tt = tt + 1 from (select-virtual tt + 1 tt from (select [tt, t] from tblx x timestamp (timestamp) where t = NULL))",
                "update tblx x set tt = tt + 1 WHERE x.t = NULL;",
                partitionedModelOf("tblx")
                        .col("t", ColumnType.TIMESTAMP)
                        .col("tt", ColumnType.TIMESTAMP)
                        .timestamp()
        );
    }


    private static TableModel partitionedModelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.DAY);
    }
}
