/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.model.*;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.ServiceLoader;

public class WhereClauseParserTest extends AbstractCairoTest {

    private SqlCompiler compiler;
    protected BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
    private static TableReader reader;
    private static TableReader noTimestampReader;
    private static TableReader unindexedReader;
    private static RecordMetadata metadata;
    private static RecordMetadata noTimestampMetadata;
    private static RecordMetadata unindexedMetadata;
    private final RpnBuilder rpn = new RpnBuilder();
    private final WhereClauseParser e = new WhereClauseParser();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = rpn::onNode;
    private final QueryModel queryModel = QueryModel.FACTORY.newInstance();
    private FunctionParser functionParser = new FunctionParser(configuration, new FunctionFactoryCache(configuration, ServiceLoader.load(FunctionFactory.class)));
    private SqlExecutionContext sqlExecutionContext;
    private CairoEngine engine;
    private QueryConstantsImpl queryConstants;

    @BeforeClass
    public static void setUp2() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL).indexed(true, 16)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL).indexed(true, 4)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4)
                    .timestamp();

            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL).indexed(true, 16)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL).indexed(true, 4)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4);
            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "z", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4)
                    .timestamp();
            CairoTestUtils.create(model);
        }

        reader = new TableReader(configuration, "x");
        metadata = reader.getMetadata();

        noTimestampReader = new TableReader(configuration, "y");
        noTimestampMetadata = noTimestampReader.getMetadata();

        unindexedReader = new TableReader(configuration, "z");
        unindexedMetadata = unindexedReader.getMetadata();
    }

    @AfterClass
    public static void tearDown2() {
        reader.close();
        noTimestampReader.close();
        unindexedReader.close();
    }

    @Before
    public void setUp1() {
        engine = new CairoEngine(configuration);
        bindVariableService = new BindVariableServiceImpl(configuration);
        compiler = new SqlCompiler(new CairoEngine(configuration));
        queryConstants = new QueryConstantsImpl(engine.getConfiguration().getMicrosecondClock());
        queryConstants.init();
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null,
                        queryConstants);
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testBadCountInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10;z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDate() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.0001110z;30m'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' > timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater2() {
        try {
            modelOf("timestamp > '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDateInInterval() {
        try {
            modelOf("timestamp = '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadEndDate() {
        try {
            modelOf("timestamp in ('2014-01-02T12:30:00.000Z', '2014-01Z')");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid date");
            Assert.assertEquals(42, e.getPosition());
        }
    }

    @Test
    public void testBadOperators() {
        testBadOperator(">");
        testBadOperator(">=");
        testBadOperator("<");
        testBadOperator("<=");
        testBadOperator("=");
        testBadOperator("!=");
    }

    @Test
    public void testBadPeriodInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;x;5'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadPeriodInInterval2() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10x;5'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadRangeInInterval() {
        try {
            modelOf("timestamp = '2014-03-01T12:30:00.000Z;x'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadStartDate() {
        try {
            modelOf("timestamp in ('2014-01Z', '2014-01-02T12:30:00.000Z')");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid date");
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testComplexInterval1() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00;2d'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:00.000000Z, hi=2015-02-25T10:00:59.999999Z}]", intervalToString(m));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;7d'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-03-02T10:00:55.000000Z}]", intervalToString(m));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}]", intervalToString(m));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval4() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z}]", intervalToString(m));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval5() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m' and timestamp != '2015-02-23T10:10:00.000Z'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:09:59.999999Z},{lo=2015-02-23T10:10:00.000001Z, hi=2015-02-23T10:30:55.000000Z}]",
                intervalToString(m));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexNow() throws Exception {
        long day = 24L * 3600 * 1000 * 1000;
        currentMicros = day;
        try {
            IntrinsicModel m = modelOf("timestamp < now()");
            TestUtils.assertEquals("[{lo=, hi=1970-01-02T00:00:00.000000Z}]",
                    intervalToString(m));
            Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
        } finally {
            currentMicros = -1;
        }
    }

    @Test
    public void testConstVsLambda() throws Exception {
        IntrinsicModel m = modelOf("ex in (1,2) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testConstVsLambda2() throws Exception {
        IntrinsicModel m = modelOf("sym in (1,2) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("sym in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testContradictingNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch2() throws Exception {
        IntrinsicModel m = modelOf("null = sym and null != sym and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch3() throws Exception {
        IntrinsicModel m = modelOf("sym = null and ex = 'blah' and sym != null");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch4() throws Exception {
        IntrinsicModel m = modelOf("sym != null and sym = null and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch1() throws Exception {
        IntrinsicModel m = modelOf("sym != 'blah' and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'blah' and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch3() throws Exception {
        IntrinsicModel m = modelOf("sym != 'blah' and sym in ('blah')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch4() throws Exception {
        IntrinsicModel m = modelOf("sym in ('blah') and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch5() throws Exception {
        IntrinsicModel m = modelOf("not (sym in ('blah')) and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch6() throws Exception {
        IntrinsicModel m = modelOf("sym = 'blah' and not (sym in ('blah'))");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch7() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and sym != 'blah' and sym != 'ho'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch8() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and not sym in ('blah', 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch9() throws Exception {
        IntrinsicModel m = modelOf("sym != 'ho' and sym in ('blah', 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[blah]", m.keyValues.toString());
        Assert.assertEquals("[32]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch10() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch11() throws Exception {
        IntrinsicModel m = modelOf("sym = null and null != sym and ex = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch12() throws Exception {
        IntrinsicModel m = modelOf("sym != 'ho' and sym in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[null]", m.keyValues.toString());
        Assert.assertEquals("[30]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch13() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and not sym in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingSearch14() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and not ex in ('blah') and not sym in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'exinnot");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testDubiousGreater() throws Exception {
        IntrinsicModel m = modelOf("ts > ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousLess() throws Exception {
        IntrinsicModel m = modelOf("ts < ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousNotEquals() throws Exception {
        IntrinsicModel m = modelOf("ts != ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsChoiceOfColumns() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'Y'ex=");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsChoiceOfColumns2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'Y'ex=");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsIndexedSearch() throws Exception {
        IntrinsicModel m = modelOf("sym ='X' and bid > 100.05");
        assertFilter(m, "100.05bid>");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsInvalidColumn() {
        try {
            modelOf("sym = 'X' and x = 'Y'");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testEqualsLambda() throws Exception {
        IntrinsicModel m = modelOf("x = (select * from x)");
        assertFilter(m, "(select-choose * column from (x))x=");
    }

    @Test
    public void testEqualsLambdaR() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) = x");
        assertFilter(m, "x(select-choose * column from (x))=");
    }

    @Test
    public void testEqualsNull() throws Exception {
        IntrinsicModel m = modelOf("sym = null");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[null]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
        Assert.assertEquals("[12]", m.keyValuePositions.toString());
    }

    @Test
    public void testEqualsOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'y' and sym in ('x','y')");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn3() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'", "ex");
        TestUtils.assertEquals("'y'sym='y''x'syminand", toRpn(m.filter));
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotEqualsOverlapWithNotIn() throws Exception {
        IntrinsicModel m = modelOf("sym != 'y' and not sym in ('x','y')");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyExcludedValues.toString());
        Assert.assertEquals("[7]", m.keyExcludedValuePositions.toString());
    }

    @Test
    public void testEqualsZeroOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'z'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsZeroOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'z' and sym in ('x','y')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testExactDate() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp < '2015-05-11T08:00:55.000Z'");
        TestUtils.assertEquals("[{lo=2015-05-10T15:03:10.000000Z, hi=2015-05-10T15:03:10.000000Z}]", intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testExactDateVsInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a', 'b', 'c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b,c]", m.keyValues.toString());
        Assert.assertEquals("[8,13,18]", m.keyValuePositions.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a]", m.keyValues.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOrInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 or timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "'2014-01-02T12:30:00.000Z''2014-01-01T12:30:00.000Z'timestampin100bid>or");
    }

    @Test
    public void testGreaterThanLambda() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) > x");
        assertFilter(m, "x(select-choose * column from (x))>");
    }

    @Test
    public void testGreaterThanLambdaR() throws Exception {
        IntrinsicModel m = modelOf("y > (select * from x)");
        assertFilter(m, "(select-choose * column from (x))y>");
    }

    @Test
    public void testLessThanLambda() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) < x");
        assertFilter(m, "x(select-choose * column from (x))<");
    }

    @Test
    public void testLessThanLambdaR() throws Exception {
        IntrinsicModel m = modelOf("z < (select * from x)");
        assertFilter(m, "(select-choose * column from (x))z<");
    }

    @Test
    public void testInNull() throws Exception {
        IntrinsicModel m = modelOf("sym in ('X', null, 'Y')");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X,null,Y]", m.keyValues.toString());
    }

    @Test
    public void testInVsEqualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp = '2014-01-01'");
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T23:59:59.999999Z}]", intervalToString(m));
    }

    @Test
    public void testIndexedFieldTooFewArgs2() throws Exception {
        assertFilter(modelOf("sym in (x)"), "xsymin");
    }

    @Test
    public void testIndexedFieldTooFewArgs3() {
        try {
            modelOf("sym in ()");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arguments");
        }
    }

    @Test
    public void testIntervalDontIntersect() throws Exception {
        // because of intervals being processed from right to left
        // code will try to intersect 'not equal' will already  existing positive interval
        // result must be zero-overlap and FALSE model
        IntrinsicModel m = modelOf("timestamp != '2015-05-11' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testIntervalGreater1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > '2014-01-01T15:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testIntervalGreater2() throws Exception {
        IntrinsicModel m = modelOf("timestamp > '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testIntervalGreater3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > x()");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        TestUtils.assertEquals("xtimestamp>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreater4() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x() > timestamp");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        TestUtils.assertEquals("timestampx>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreater5() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp > '2014-01-01T15:30:00.000Z'");
        Assert.assertFalse(m.hasIntervals());
        TestUtils.assertEquals("'2014-01-01T15:30:00.000Z'timestamp>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreaterOrEq1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp >= '2014-01-01T15:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testIntervalGreaterOrEq2() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testIntervalLessNoTimestamp() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp < '2014-01-01T15:30:00.000Z'");
        Assert.assertFalse(m.hasIntervals());
        TestUtils.assertEquals("'2014-01-01T15:30:00.000Z'timestamp<", toRpn(m.filter));
    }

    @Test
    public void testIntervalSourceDay() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;2d;5'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z},{lo=2015-02-25T10:00:55.000000Z, hi=2015-02-25T10:30:55.000000Z},{lo=2015-02-27T10:00:55.000000Z, hi=2015-02-27T10:30:55.000000Z},{lo=2015-03-01T10:00:55.000000Z, hi=2015-03-01T10:30:55.000000Z},{lo=2015-03-03T10:00:55.000000Z, hi=2015-03-03T10:30:55.000000Z}]",
                intervalToString(m));
    }

    @Test
    public void testIntervalSourceHour() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;10m;3h;10'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:10:55.000000Z}," +
                "{lo=2015-02-23T13:00:55.000000Z, hi=2015-02-23T13:10:55.000000Z}," +
                "{lo=2015-02-23T16:00:55.000000Z, hi=2015-02-23T16:10:55.000000Z}," +
                "{lo=2015-02-23T19:00:55.000000Z, hi=2015-02-23T19:10:55.000000Z}," +
                "{lo=2015-02-23T22:00:55.000000Z, hi=2015-02-23T22:10:55.000000Z}," +
                "{lo=2015-02-24T01:00:55.000000Z, hi=2015-02-24T01:10:55.000000Z}," +
                "{lo=2015-02-24T04:00:55.000000Z, hi=2015-02-24T04:10:55.000000Z}," +
                "{lo=2015-02-24T07:00:55.000000Z, hi=2015-02-24T07:10:55.000000Z}," +
                "{lo=2015-02-24T10:00:55.000000Z, hi=2015-02-24T10:10:55.000000Z}," +
                "{lo=2015-02-24T13:00:55.000000Z, hi=2015-02-24T13:10:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m));
    }

    @Test
    public void testIntervalSourceMin() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s;15m;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}," +
                "{lo=2015-02-23T10:15:55.000000Z, hi=2015-02-23T10:16:10.000000Z}," +
                "{lo=2015-02-23T10:30:55.000000Z, hi=2015-02-23T10:31:10.000000Z}," +
                "{lo=2015-02-23T10:45:55.000000Z, hi=2015-02-23T10:46:10.000000Z}," +
                "{lo=2015-02-23T11:00:55.000000Z, hi=2015-02-23T11:01:10.000000Z}]";
        TestUtils.assertEquals(expected, intervalToString(m));
    }

    @Test
    public void testIntervalSourceMonth() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;2h;2M;3'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T12:00:55.000000Z}," +
                "{lo=2015-04-23T10:00:55.000000Z, hi=2015-04-23T12:00:55.000000Z}," +
                "{lo=2015-06-23T10:00:55.000000Z, hi=2015-06-23T12:00:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m));
    }

    @Test
    public void testIntervalSourceSec() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;5s;30s;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:00.000000Z}," +
                "{lo=2015-02-23T10:01:25.000000Z, hi=2015-02-23T10:01:30.000000Z}," +
                "{lo=2015-02-23T10:01:55.000000Z, hi=2015-02-23T10:02:00.000000Z}," +
                "{lo=2015-02-23T10:02:25.000000Z, hi=2015-02-23T10:02:30.000000Z}," +
                "{lo=2015-02-23T10:02:55.000000Z, hi=2015-02-23T10:03:00.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m));
    }

    @Test
    public void testIntervalSourceYear() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;1d;1y;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-24T10:00:55.000000Z}," +
                "{lo=2016-02-23T10:00:55.000000Z, hi=2016-02-24T10:00:55.000000Z}," +
                "{lo=2017-02-23T10:00:55.000000Z, hi=2017-02-24T10:00:55.000000Z}," +
                "{lo=2018-02-23T10:00:55.000000Z, hi=2018-02-24T10:00:55.000000Z}," +
                "{lo=2019-02-23T10:00:55.000000Z, hi=2019-02-24T10:00:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m));
    }

    @Test
    public void testIntervalTooFewArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arg");
        }
    }

    @Test
    public void testIntervalTooFewArgs2() {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arg");
        }
    }

    @Test
    public void testIntervalTooManyArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\", \"2014-01-03T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too many arg");
        }
    }

    @Test
    public void testIntrinsicPickup() throws Exception {
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' and sym in ('A', 'B') or ex = 'D'"), "'D'ex='B''A'symin'2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=andor");
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' or ex = 'D' and sym in ('A', 'B')"), "'D'ex='2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=or");
    }

    @Test(expected = SqlException.class)
    public void testInvalidIntervalSource1() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d'");
    }

    @Test(expected = SqlException.class)
    public void testInvalidIntervalSource2() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4;4'");
    }

    @Test
    public void testLambdaVsConst() throws Exception {
        IntrinsicModel m = modelOf("sym in (select a from xyz) and ex in (1,2)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testLambdaVsLambda() throws Exception {
        IntrinsicModel m = modelOf("ex in (select * from abc) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (select-choose * column from (abc))", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testLiteralNotInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("not sym in ('a', z) and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z'a'syminnot");
    }

    @Test
    public void testLessInvalidDate() {
        try {
            modelOf("timestamp < '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testLessInvalidDate2() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' < timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }
    }

    @Test
    public void testLessNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("timestamp < x");
        Assert.assertFalse(m.hasIntervals());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("xtimestamp<", toRpn(m.filter));
    }

    @Test
    public void testLessNonConstant2() throws SqlException {
        IntrinsicModel m = modelOf("x < timestamp");
        Assert.assertFalse(m.hasIntervals());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("timestampx<", toRpn(m.filter));
    }

    @Test
    public void testListOfValuesNegativeOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('c')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('c', 'd', 'e'))");
        Assert.assertEquals("[a,z]", m.keyValues.toString());
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause2() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'd', 'e'))");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[z]", m.keyValues.toString());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'z', 'e'))");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesPositiveOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testListOfValuesPositiveOverlapQuoteIndifference() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testLiteralInInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', c)");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "c'2014-01-01T12:30:00.000Z'timestampin");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a', z) and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z'a'symin");
    }

    @Test
    public void testNotInLambdaVsConst() throws Exception {
        IntrinsicModel m = modelOf("not (sym in (select a from xyz)) and not (ex in (1,2))");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[1,2]", m.keyExcludedValues.toString());
        assertFilter(m, "(select-choose a from (xyz))syminnot");
    }

    @Test
    public void testLiteralInListOfValuesInvalidColumn() {
        try {
            modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x in ('a', z)");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(74, e.getPosition());
        }
    }

    @Test
    public void testManualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp < '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]", intervalToString(m));
    }

    @Test
    public void testManualIntervalInverted() throws Exception {
        IntrinsicModel m = modelOf("'2014-01-02T12:30:00.000Z' > timestamp and '2014-01-01T15:30:00.000Z' <= timestamp ");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]", intervalToString(m));
    }

    @Test
    public void testMultipleAnds() throws Exception {
        IntrinsicModel m = modelOf("a > 10 and b > 20 and (c > 100 and d < 20 and bid = 30)");
        assertFilter(m, "30bid=20d<100c>andand20b>10a>andand");
    }

    @Test
    public void testNestedFunctionTest() throws Exception {
        IntrinsicModel m = modelOf("substr(parse(x, 1, 3), 2, 4)");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "4231xparsesubstr");
    }

    @Test
    public void testNoIntrinsics() throws Exception {
        IntrinsicModel m = modelOf("a > 10 or b > 20");
        Assert.assertFalse(m.hasIntervals());
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "20b>10a>or");
    }

    @Test
    public void testNotEqualInvalidColumn() {
        try {
            modelOf("ex != null and abb != 'blah'");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid column");
            Assert.assertEquals(15, e.getPosition());
        }
    }

    @Test
    public void testNotEqualPreferredColumn() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'", "ex");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=nullsym!=nullsym=andand");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotEqualsDoesNotOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym != 'z' and ex != 'blah'");
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[x,y]", m.keyValues.toString());
        Assert.assertEquals("[8,12]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym != 'y' and ex != 'blah'");
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[x]", m.keyValues.toString());
        Assert.assertEquals("[8]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotInIntervalIntersect() throws Exception {
        IntrinsicModel m = modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z')) and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalIntersect2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-11' and not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z'))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalIntersect3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-11' and not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z')) and not (timestamp in ('2015-05-11T12:00:00.000Z', '2015-05-11T14:00:00.000Z')))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T11:59:59.999999Z},{lo=2015-05-11T14:00:00.000001Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalInvalidHi() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z', 'abc')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(48, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalInvalidLo() {
        try {
            modelOf("not (timestamp in  ('abc','2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(20, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("not (timestamp in  (x, 'abc')) and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T23:59:59.999999Z}]", intervalToString(m));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("'abc'xtimestampinnot", toRpn(m.filter));
    }

    @Test
    public void testNotInIntervalNonLiteral() {
        try {
            modelOf("not (timestamp() in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Column name");
        }
    }

    @Test
    public void testNotInIntervalTooFew() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too few");
        }
    }

    @Test
    public void testNotInIntervalTooMany() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too many");
        }
    }

    @Test
    public void testNotInInvalidColumn() {
        try {
            modelOf("not (xyz in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            Assert.assertEquals(5, e.getPosition());
        }
    }

    @Test
    public void testNotInTooFew() {
        try {
            modelOf("not (ex in  ()) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too few");
            Assert.assertEquals(8, e.getPosition());
        }
    }

    @Test
    public void testOr() throws Exception {
        IntrinsicModel m = modelOf("(sym = 'X' or sym = 'Y') and bid > 10");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "10bid>'Y'sym='X'sym=orand");
    }

    @Test
    public void testOrNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null or sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "nullsym!=nullsym=or");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
        Assert.assertEquals("[blah]", m.keyExcludedValues.toString());
        Assert.assertEquals("[36]", m.keyExcludedValuePositions.toString());
    }

    @Test
    public void testOrNullSearch2() throws Exception {
        IntrinsicModel m = modelOf("sym = null or sym != null and ex = 'blah'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "nullsym!=nullsym=or");
        Assert.assertEquals("[blah]", m.keyValues.toString());
        Assert.assertEquals("[35]", m.keyValuePositions.toString());
    }

    @Test
    public void testPreferredColumn() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testPreferredColumn2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testPreferredColumn3() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testSimpleBetweenAndInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testTwoBetweenIntervalsWithOr() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' or timestamp between '2014-02-01T12:30:00.000Z' and '2014-02-02T12:30:00.000Z'");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "'2014-02-02T12:30:00.000Z''2014-02-01T12:30:00.000Z'timestampbetween'2014-01-02T12:30:00.000Z''2014-01-01T12:30:00.000Z'timestampbetweenor");
    }

    @Test
    public void testBetweenInFunctionOfThreeArgs() throws Exception {
        IntrinsicModel m = modelOf("func(2, timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z', 'abc')");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "'abc''2014-01-02T12:30:00.000Z''2014-01-01T12:30:00.000Z'timestampbetween2func");
    }

    @Test
    public void testBetweenInFunctionOfThreeArgsDangling() {
        try {
            modelOf("func(2, timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z',)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(84, e.getPosition());
            TestUtils.assertEquals("missing arguments", e.getFlyweightMessage());
        }
    }

    @Test
    public void testTwoBetweenIntervalsForIntColumn() throws Exception {
        IntrinsicModel m = modelOf("bidSize between 5 and 10 ");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "105bidSizebetween");
    }

    @Test
    public void testTwoBetweenIntervalsForDoubleColumn() throws Exception {
        IntrinsicModel m = modelOf("bid between 5 and 10 ");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "105bidbetween");
    }

    @Test
    public void testTwoBetweenIntervalsForExpression() {
        try {
            modelOf("ask between bid+ask/2 and 10 ");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between/and parameters must be constants");
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testTwoBetweenIntervalsForExpression2() {
        try {
            modelOf("ask between 1 and bid+ask/2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between/and parameters must be constants");
            Assert.assertEquals(18, e.getPosition());
        }
    }

    @Test
    public void testTwoNestedBetween1() {
        try {
            modelOf("ask between between 1 and 2 and bid+ask/2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too few");
            Assert.assertEquals(4, e.getPosition());
        }
    }

    @Test
    public void testTwoNestedBetween2() {
        try {
            modelOf("ask between (between 1 and 2) and bid+ask/2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too few");
            Assert.assertEquals(13, e.getPosition());
        }
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParam() {
        try {
            modelOf("timestamp between case when true then '2014-01-02T12:30:00.000Z' else '2014-01-02T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between/and parameters must be constants");
            Assert.assertEquals(18, e.getPosition());
        }
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParam2() {
        try {
            modelOf("timestamp between '2014-01-02T12:30:00.000Z' and case when true then '2014-01-02T12:30:00.000Z' else '2014-01-02T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between/and parameters must be constants");
            Assert.assertEquals(49, e.getPosition());
        }
    }

    @Test
    public void testTwoBetweenIntervalsWithAnd() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp between '2014-01-01T16:30:00.000Z' and '2014-01-05T12:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testTwoIntervalsWithOr() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ( '2014-01-01T12:30:00.000Z' ,  '2014-01-02T12:30:00.000Z') or timestamp in ('2014-02-01T12:30:00.000Z', '2014-02-02T12:30:00.000Z')");
        Assert.assertFalse(m.hasIntervals());
        assertFilter(m, "'2014-02-02T12:30:00.000Z''2014-02-01T12:30:00.000Z'timestampin'2014-01-02T12:30:00.000Z''2014-01-01T12:30:00.000Z'timestampinor");
    }

    @Test
    public void testTwoIntervalsWithAnd() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp in ('2014-01-01T16:30:00.000Z', '2014-01-05T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testSimpleLambda() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz)");
        Assert.assertNotNull(m.keySubQuery);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
//        assertFilter(m, "110ask<100bid>'c'exinandand");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testThreeIntrinsics2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testTwoDiffColLambdas() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz) and ex in (select  * from kkk)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals(ExpressionNode.QUERY, m.filter.rhs.type);
    }

    @Test
    public void testTwoExactMatchDifferentDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoIntervalSources() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;5' and timestamp = '2015-06-20T13:25:00.000Z;10m;2d;5'");
        TestUtils.assertEquals("[]", intervalToString(m));
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp in ('2014-01-01T16:30:00.000Z', '2014-01-05T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m));
    }

    @Test
    public void testTwoSameColLambdas() {
        try {
            modelOf("sym in (select * from xyz) and sym in (select * from kkk)");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Multiple lambda");
        }
    }

    @Test
    public void testUnindexedEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym = 'ABC'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2)", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[ABC]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2)", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[1,2]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredInVsIndexed() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2) and ex in ('XYZ')", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        TestUtils.assertEquals("ex in 'XYZ'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[1,2]", m.keyValues.toString());
    }

    private void assertFilter(IntrinsicModel m, CharSequence expected) throws SqlException {
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(expected, toRpn(m.filter));
    }

    private IntrinsicModel modelOf(CharSequence seq) throws SqlException {
        return modelOf(seq, null);
    }

    private IntrinsicModel modelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), metadata, preferredColumn, metadata.getTimestampIndex(), functionParser, metadata, sqlExecutionContext);
    }

    private IntrinsicModel noTimestampModelOf(CharSequence seq) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), noTimestampMetadata, null, noTimestampMetadata.getTimestampIndex(), functionParser, metadata, sqlExecutionContext);
    }

    private void testBadOperator(String op) {
        try {
            modelOf("sum(ts) " + op);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(8, e.getPosition());
        }

        try {
            modelOf(op + " sum(ts)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }

    }

    private CharSequence toRpn(ExpressionNode node) throws SqlException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }

    private IntrinsicModel unindexedModelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), unindexedMetadata, preferredColumn, unindexedMetadata.getTimestampIndex(), functionParser, metadata, sqlExecutionContext);
    }

    private CharSequence intervalToString(IntrinsicModel model) {
        if (!model.hasIntervals()) return "";
        RuntimeIntrinsicIntervalModel sm = model.getIntervalModel();
        return GriffinParserTestUtils.intervalToString(sm.calculateIntervals(sqlExecutionContext));
    }
}
