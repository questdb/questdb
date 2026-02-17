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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.WhereClauseSymbolEstimator;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WhereClauseSymbolEstimatorTest extends AbstractCairoTest {

    private static RecordMetadata metadata;
    private static TableReader reader;
    private final WhereClauseSymbolEstimator e = new WhereClauseSymbolEstimator();
    private final QueryModel queryModel = QueryModel.FACTORY.newInstance();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();

        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5)
                .timestamp();
        AbstractCairoTest.create(model);

        reader = newOffPoolReader(configuration, "x");
        metadata = reader.getMetadata();
    }

    @AfterClass
    public static void tearDownStatic() {
        reader = Misc.free(reader);
        metadata = null;
        AbstractCairoTest.tearDownStatic();
    }

    @Override
    public void tearDown() {
        super.tearDown(false);
        e.clear();
    }

    @Test
    public void testEstimateMultipleColumns1() throws SqlException {
        Assert.assertEquals(
                intList(1, 3),
                estimate("ex in ('x','y','z') and sym = 'a'", "sym", "ex")
        );
    }

    @Test
    public void testEstimateMultipleColumns2() throws SqlException {
        Assert.assertEquals(
                intList(1, 2),
                estimate("bidSize = 10 and ex = 'x' and sym in ('a','b')", "ex", "sym")
        );
    }

    @Test
    public void testEstimateMultipleColumnsGaveUp1() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE, 4),
                estimate("bidSize != 10 and sym in ('a','b','c','d')", "ex", "sym")
        );
    }

    @Test
    public void testEstimateMultipleColumnsGaveUp2() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE, Integer.MAX_VALUE),
                estimate("ex not in ('x','y','z') and sym = 'a'", "ex", "sym")
        );
    }

    @Test
    public void testEstimateMultipleColumnsGaveUp3() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE, Integer.MAX_VALUE),
                estimate("ex != 'x' and sym = 'a'", "ex", "sym")
        );
    }

    @Test
    public void testEstimateMultipleColumnsGaveUp4() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE, Integer.MAX_VALUE),
                estimate("'x' != ex and sym = 'a'", "ex", "sym")
        );
    }

    @Test
    public void testEstimateNullFilter() throws SqlException {
        Assert.assertNull(e.estimate(column -> column, null, metadata, new IntList()));
    }

    @Test
    public void testEstimateSingleColumn1() throws SqlException {
        Assert.assertEquals(
                intList(1),
                estimate("sym = 'a'", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumn2() throws SqlException {
        Assert.assertEquals(
                intList(1),
                estimate("'a' = sym", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumn3() throws SqlException {
        Assert.assertEquals(
                intList(3),
                estimate("sym in ('a','b','c')", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumn4() throws SqlException {
        Assert.assertEquals(
                intList(2),
                estimate("sym in (:sym1,:sym2)", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumn5() throws SqlException {
        Assert.assertEquals(
                intList(1),
                estimate("2*bidSize > 42 and sym = 'a'", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnGaveUp1() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("ex in (select * from xyz) and ex = 'a'", "ex")
        );
    }

    @Test
    public void testEstimateSingleColumnGaveUp2() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("sym = 'a' and not (sym = 'b')", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnGaveUp3() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("sym = 'a' or sym in ('b')", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnGaveUp4() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("sym = ex", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnNotInFilter1() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("ex = 'a'", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnNotInFilter2() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("ex != 'a'", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnNotInFilter3() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("ex in ('a')", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnNotInFilter4() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("ex not in ('a')", "sym")
        );
    }

    @Test
    public void testEstimateSingleColumnNotInFilter6() throws SqlException {
        Assert.assertEquals(
                intList(Integer.MAX_VALUE),
                estimate("now() = 1 and now() != 1 and concat('a','b') in ('a') and concat('a','b') not in ('a')", "sym")
        );
    }

    @Test
    public void testEstimateUnknownColumnThrows1() {
        try {
            estimate("sym0 = 'a'", "sym");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
        }
    }

    @Test
    public void testEstimateUnknownColumnThrows2() {
        try {
            estimate("sym0 in ('a')", "sym");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
        }
    }

    @Test
    public void testEstimateUnknownColumnThrows3() {
        try {
            estimate("sym0 != 'a'", "sym");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
        }
    }

    @Test
    public void testEstimateUnknownColumnThrows4() {
        try {
            estimate("sym0 not in ('a')", "sym");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
        }
    }

    private static IntList intList(Integer... values) {
        IntList list = new IntList(values.length);
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    private IntList estimate(CharSequence whereClause, String... columns) throws SqlException {
        queryModel.clear();
        IntList columnIndexes = new IntList(columns.length);
        for (String column : columns) {
            columnIndexes.add(metadata.getColumnIndexQuiet(column));
        }
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            return e.estimate(
                    column -> column,
                    compiler.testParseExpression(whereClause, queryModel),
                    metadata,
                    columnIndexes
            );
        }
    }
}
