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
import org.junit.Test;

public class SqlParserUpdateTest extends AbstractSqlParserTest {
    @Test
    public void testUpdateSingleTableWithWhere() throws Exception {
        assertUpdate("update x set tt = t from (x where t > '2005-04-02T12:00:00')",
                "update x set tt = t where t > '2005-04-02T12:00:00'",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testUpdateSingleTableToConst() throws Exception {
        assertUpdate("update x set tt = 1 from (x)",
                "update x set tt = 1",
                modelOf("x").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testUpdateSingleTableWithAlias() throws Exception {
        assertUpdate("update tblx as x set tt = tt + 1 from (tblx x where t = NULL)",
                "update tblx x set tt = tt + 1 WHERE x.t = NULL",
                modelOf("tblx").col("t", ColumnType.TIMESTAMP).col("tt", ColumnType.TIMESTAMP));
    }

    @Test
    public void testUpdateSingleTableWithJoinInFrom() throws Exception {
        assertUpdate("update tblx set tt = 1 from (tblx join tbly y on y = x where x > 10)",
                "update tblx set tt = 1 from tbly y where x = y and x > 10",
                modelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testSelectSingleTableWithJoinInFrom() throws Exception {
        assertQuery("select-virtual rowid() rowid, 1 tt from (select [x] from tblx x join select [y] from tbly y on y.y = x.x where x > 10) x",
                "select rowid(), 1 as tt from tblx x, tbly y where x.x = y.y and x > 10",
                modelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }

    @Test
    public void testUpdateWithJoinAndTableAlias() throws Exception {
        assertUpdate("update tblx as xx set tt = 1 from (tblx xx join tbly y on y = xx.x where x > 10)",
                "update tblx as xx set tt = 1 from tbly y where xx.x = y and x > 10",
                modelOf("tblx").col("t", ColumnType.TIMESTAMP).col("x", ColumnType.INT),
                modelOf("tbly").col("t", ColumnType.TIMESTAMP).col("y", ColumnType.INT));
    }
}
