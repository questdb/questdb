/*+*****************************************************************************
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
                "update tblx set y = y from tbly y where tblx.x = y.y and tblx.x > 10",
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
        assertQuery("update tblx set tt = tt + 1 WHERE t = NULL;")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [tt + 1 AS tt]
                          Filter
                            predicate: t = null
                            Scan
                              table: tblx
                              columns: [tt, t]
                        """);
    }

    @Test
    public void testUpdateSingleTableToBindVariable() throws Exception {
        assertQuery("update x set tt = $1")
                .ddl("CREATE TABLE x (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [$1 AS tt]
                          Scan
                            table: x
                            columns: []
                        """);
    }

    @Test
    public void testUpdateSingleTableToConst() throws Exception {
        assertQuery("update x set tt = 1")
                .ddl("CREATE TABLE x (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Scan
                            table: x
                            columns: []
                        """);
    }

    @Test
    public void testUpdateSingleTableWithAlias() throws Exception {
        assertQuery("update tblx x set tt = tt + 1 WHERE x.t = NULL")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [tt + 1 AS tt]
                          Filter
                            predicate: t = null
                            Scan
                              table: tblx
                              columns: [tt, t]
                        """);
    }

    @Test
    public void testUpdateSingleTableWithJoinAndConstFiltering() throws Exception {
        assertQuery("update tblx set tt = 1 from tbly y where x = y and x > 10 and 100 > 100")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Scan
                                table: tblx
                                columns: [x]
                            INNER y
                              keys: [y.y = tblx.x]
                              filter: false
                              Scan
                                table: tbly
                                columns: [y]
                        """);
    }

    @Test
    public void testUpdateSingleTableWithJoinAndFiltering() throws Exception {
        assertQuery("update tblx set tt = 1 from tbly y where x = y and x > 10 and y.t > 100")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Filter
                                predicate: tblx.x > 10
                                Scan
                                  table: tblx
                                  columns: [x]
                            INNER y
                              keys: [y.y = tblx.x]
                              Filter
                                predicate: y.t > 100
                                Scan
                                  table: tbly
                                  columns: [t, y]
                        """);
    }

    @Test
    public void testUpdateSingleTableWithJoinAndNestedSampleBy() throws Exception {
        assertQuery("update tblx set tt = 1 from (select ts, first(y) as y from tbly SAMPLE BY 1h ALIGN TO FIRST OBSERVATION) y where x = y")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Scan
                                table: tblx
                                columns: [x]
                            INNER y
                              keys: [y.y = tblx.x]
                              Project
                                columns: [y]
                                SampleBy
                                  period: 1h
                                  keys: [ts]
                                  values: [first(y) AS y]
                                  Scan
                                    table: tbly
                                    columns: [y, ts]
                        """);

        assertQuery("update tblx set tt = 1 from (select ts, first(y) as y from tbly SAMPLE BY 1h ALIGN TO CALENDAR) y where x = y")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Scan
                                table: tblx
                                columns: [x]
                            INNER y
                              keys: [y.y = tblx.x]
                              Sort
                                keys: [y.ts]
                                Project
                                  columns: [ts, y]
                                  Aggregate
                                    keys: [timestamp_floor_utc('1h', ts, null, '00:00', null) AS ts]
                                    values: [first(y) AS y]
                                    Scan
                                      table: tbly
                                      columns: [y, ts]
                        """);
    }

    @Test
    public void testUpdateSingleTableWithJoinInFrom() throws Exception {
        assertQuery("update tblx set tt = tt + 1 from tbly y where x = y and x > 10")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [tblx.tt + 1 AS tt]
                          Join
                            Master tblx
                              Filter
                                predicate: tblx.x > 10
                                Scan
                                  table: tblx
                                  columns: [x, tt]
                            INNER y
                              keys: [y.y = tblx.x]
                              Scan
                                table: tbly
                                columns: [y]
                        """);
    }

    @Test
    public void testUpdateSingleTableWithWhere() throws Exception {
        assertQuery("update x set tt = t where t > '2005-04-02T12:00:00'")
                .ddl("CREATE TABLE x (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [t AS tt]
                          Filter
                            predicate: t > '2005-04-02T12:00:00'::TIMESTAMP
                            Scan
                              table: x
                              columns: [t]
                        """);
    }

    @Test
    public void testUpdateTwoColumnsToConst() throws Exception {
        assertQuery("update x set tt = 1, x = 2")
                .ddl("CREATE TABLE x (t TIMESTAMP, tt TIMESTAMP, x INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt, 2 AS x]
                          Scan
                            table: x
                            columns: []
                        """);
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
        assertQuery("update tblx set tt = 1 from tbly y")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Scan
                                table: tblx
                                columns: []
                            CROSS y
                              Scan
                                table: tbly
                                columns: []
                        """);

        assertQuery("update tblx set tt = 1 from tbly y;")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master tblx
                              Scan
                                table: tblx
                                columns: []
                            CROSS y
                              Scan
                                table: tbly
                                columns: []
                        """);
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
        assertQuery("update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master xx
                              Filter
                                predicate: xx.x > 10
                                Scan
                                  table: tblx
                                  columns: [x]
                            INNER y
                              keys: [y.y = xx.x]
                              Scan
                                table: tbly
                                columns: [y]
                        """);
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
        assertQuery("update tblx as xx set tt = 1 from (tbly LIMIT 10) y where xx.x = y and x > 10")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, x INT, tt INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY", "CREATE TABLE tbly (t TIMESTAMP, y INT)")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Join
                            Master xx
                              Filter
                                predicate: xx.x > 10
                                Scan
                                  table: tblx
                                  columns: [x]
                            INNER y
                              keys: [y.y = xx.x]
                              Limit
                                lo: 10
                                Project
                                  columns: [y]
                                  Scan
                                    table: tbly
                                    columns: [y]
                        """);
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
        assertQuery("update x set tt = 1;")
                .ddl("CREATE TABLE x (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [1 AS tt]
                          Scan
                            table: x
                            columns: []
                        """);
    }

    @Test
    public void testUpdateWithWhereAndSemicolon() throws Exception {
        assertQuery("update tblx x set tt = tt + 1 WHERE x.t = NULL;")
                .ddl("CREATE TABLE tblx (t TIMESTAMP, tt TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY")
                .assertsLogicalPlan("""
                        Project
                          columns: [tt + 1 AS tt]
                          Filter
                            predicate: t = null
                            Scan
                              table: tblx
                              columns: [tt, t]
                        """);
    }


    private static TableModel partitionedModelOf(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.DAY);
    }
}
