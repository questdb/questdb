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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.WhereClauseParser;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WhereClauseParserTest extends AbstractCairoTest {
    private static RecordMetadata metadata;
    private static RecordMetadata metadataNanos;
    private static RecordMetadata noDesignatedTimestampNorIdxMetadata;
    private static RecordMetadata noDesignatedTimestampNorIdxMetadataNanos;
    private static TableReader noDesignatedTimestampNorIdxReader;
    private static TableReader noDesignatedTimestampNorIdxReaderNanos;
    private static RecordMetadata noTimestampMetadata;
    private static TableReader noTimestampReader;
    private static RecordMetadata nonEmptyMetadata;
    private static RecordMetadata nonEmptyMetadataNanos;
    private static TableReader nonEmptyReader;
    private static TableReader nonEmptyReaderNanos;
    private static TableReader reader;
    private static TableReader readerNanos;
    private static RecordMetadata unindexedMetadata;
    private static RecordMetadata unindexedMetadataNanos;
    private static TableReader unindexedReader;
    private static TableReader unindexedReaderNanos;
    private final WhereClauseParser e = new WhereClauseParser();
    private final FunctionParser functionParser = new FunctionParser(
            configuration,
            engine.getFunctionFactoryCache()
    );
    private final QueryModel queryModel = QueryModel.FACTORY.newInstance();
    private final RpnBuilder rpn = new RpnBuilder();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = rpn::onNode;
    private final TestTimestampType timestampType;
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();

    public WhereClauseParserTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();

        // same as x but with different number of values in symbol maps
        TableModel model = new TableModel(configuration, "v", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1).indexed(true, 16)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4).indexed(true, 4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestamp();
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "v_ns", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1).indexed(true, 16)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4).indexed(true, 4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestampNs();
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "w", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL)
                .col("ex", ColumnType.SYMBOL)
                .col("timestamp", ColumnType.TIMESTAMP);
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "w_ns", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL)
                .col("ex", ColumnType.SYMBOL)
                .col("timestamp", ColumnType.TIMESTAMP_NANO);
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "x", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1).indexed(true, 16)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4).indexed(true, 4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestamp();
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "x_ns", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1).indexed(true, 16)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4).indexed(true, 4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestampNs();
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "y", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL).symbolCapacity(1).indexed(true, 16)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(4).indexed(true, 4)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4);
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "z", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestamp();
        AbstractCairoTest.create(model);

        model = new TableModel(configuration, "z_ns", PartitionBy.NONE);
        model.col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(5).indexed(true, 4)
                .timestampNs();
        AbstractCairoTest.create(model);

        try (TableWriter writer = newOffPoolWriter(configuration, "v")) {
            TableWriter.Row row = writer.newRow(0);
            row.putSym(0, "sym1");
            row.putSym(5, "mode1");
            row.append();

            row = writer.newRow(1);
            row.putSym(0, "sym2");
            row.putSym(5, "mode1");
            row.append();

            writer.commit();
        }

        try (TableWriter writer = newOffPoolWriter(configuration, "v_ns")) {
            TableWriter.Row row = writer.newRow(0);
            row.putSym(0, "sym1");
            row.putSym(5, "mode1");
            row.append();

            row = writer.newRow(1);
            row.putSym(0, "sym2");
            row.putSym(5, "mode1");
            row.append();

            writer.commit();
        }

        reader = newOffPoolReader(configuration, "x");
        metadata = reader.getMetadata();

        readerNanos = newOffPoolReader(configuration, "x_ns");
        metadataNanos = readerNanos.getMetadata();

        noTimestampReader = newOffPoolReader(configuration, "y");
        noTimestampMetadata = noTimestampReader.getMetadata();

        unindexedReader = newOffPoolReader(configuration, "z");
        unindexedMetadata = unindexedReader.getMetadata();

        unindexedReaderNanos = newOffPoolReader(configuration, "z_ns");
        unindexedMetadataNanos = unindexedReaderNanos.getMetadata();

        noDesignatedTimestampNorIdxReader = newOffPoolReader(configuration, "w");
        noDesignatedTimestampNorIdxMetadata = noDesignatedTimestampNorIdxReader.getMetadata();

        noDesignatedTimestampNorIdxReaderNanos = newOffPoolReader(configuration, "w_ns");
        noDesignatedTimestampNorIdxMetadataNanos = noDesignatedTimestampNorIdxReaderNanos.getMetadata();

        nonEmptyReader = newOffPoolReader(configuration, "v");
        nonEmptyMetadata = nonEmptyReader.getMetadata();

        nonEmptyReaderNanos = newOffPoolReader(configuration, "v_ns");
        nonEmptyMetadataNanos = nonEmptyReaderNanos.getMetadata();
    }

    @AfterClass
    public static void tearDownStatic() {
        reader = Misc.free(reader);
        metadata = null;
        reader = Misc.free(readerNanos);
        metadataNanos = null;
        noTimestampReader = Misc.free(noTimestampReader);
        noTimestampMetadata = null;
        unindexedReader = Misc.free(unindexedReader);
        unindexedMetadata = null;
        noDesignatedTimestampNorIdxReader = Misc.free(noDesignatedTimestampNorIdxReader);
        noDesignatedTimestampNorIdxMetadata = null;
        nonEmptyReader = Misc.free(nonEmptyReader);
        nonEmptyMetadata = null;
        unindexedReaderNanos = Misc.free(unindexedReaderNanos);
        unindexedMetadataNanos = null;
        noDesignatedTimestampNorIdxReaderNanos = Misc.free(noDesignatedTimestampNorIdxReaderNanos);
        noDesignatedTimestampNorIdxMetadataNanos = null;
        nonEmptyReaderNanos = Misc.free(nonEmptyReaderNanos);
        nonEmptyMetadataNanos = null;
        AbstractCairoTest.tearDownStatic();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Override
    public void tearDown() {
        super.tearDown(false);
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, "100 bid >");
        Assert.assertNull(m.keyColumn);
        Assert.assertTrue(m.hasIntervalFilters());
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testAndBranchWithNonIndexedFieldNoDesignatedTimestamp() throws Exception {
        IntrinsicModel m = noDesignatedTimestampNotIdxModelOf(
                "timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "100 bid > '2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp between and");
        Assert.assertNull(m.keyColumn);
        Assert.assertFalse(m.hasIntervalFilters());
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testAndBranchWithNonIndexedFieldNoDesignatedTimestampVarchar() throws Exception {
        IntrinsicModel m = noDesignatedTimestampNotIdxModelOf(
                "timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and bid > 100");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "100 bid > varchar '2014-01-02T12:30:00.000Z' cast varchar '2014-01-01T12:30:00.000Z' cast timestamp between and");
        Assert.assertNull(m.keyColumn);
        Assert.assertFalse(m.hasIntervalFilters());
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testAndBranchWithNonIndexedFieldVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and bid > 100");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, "100 bid >");
        Assert.assertNull(m.keyColumn);
        Assert.assertTrue(m.hasIntervalFilters());
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testBadConstFunctionDateGreater() throws SqlException {
        IntrinsicModel m = modelOf("timestamp > to_date('2015-02-AB', 'yyyy-MM-dd')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testBadConstFunctionDateGreaterVarchar() throws SqlException {
        IntrinsicModel m = modelOf("timestamp > to_date('2015-02-AB'::varchar, 'yyyy-MM-dd')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testBadConstFunctionDateLess() throws SqlException {
        IntrinsicModel m = modelOf("timestamp < to_date('2015-02-AA', 'yyyy-MM-dd')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testBadConstFunctionDateLessVarchar() throws SqlException {
        IntrinsicModel m = modelOf("timestamp < to_date('2015-02-AA'::varchar, 'yyyy-MM-dd')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testBadCountInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10;z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] not a timestamp, use IN keyword with intervals", e.getMessage());
        }
    }

    @Test
    public void testBadCountInIntervalVarchar() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10;z'::varchar");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[47] Not a date, use IN keyword with intervals", e.getMessage());
        }
    }

    @Test
    public void testBadDate() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.0001110z;30m'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] not a timestamp, use IN keyword with intervals", e.getMessage());
        }
    }

    @Test
    public void testBadDateInGreater() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' > timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            Assert.assertEquals("[0] Invalid date [str='2014-0x-01T12:30:00.000Z']", e.getMessage());
        }
    }

    @Test
    public void testBadDateInGreater2() {
        try {
            modelOf("timestamp > '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] Invalid date [str='2014-0x-01T12:30:00.000Z']", e.getMessage());
        }
    }

    @Test
    public void testBadDateInGreater2Varchar() {
        try {
            modelOf("timestamp > '2014-0x-01T12:30:00.000Z'::varchar");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[38] Invalid date [str=2014-0x-01T12:30:00.000Z]", e.getMessage());
        }
    }

    @Test
    public void testBadDateInGreaterVarchar() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z'::varchar > timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(26, e.getPosition());
            // todo: string literal produce quoted string ([str='2014-0x-01T12:30:00.000Z']) while
            // casting to varchar produce unquoted string ([str=2014-0x-01T12:30:00.000Z])
            // fix this inconsistency
            Assert.assertEquals("[26] Invalid date [str=2014-0x-01T12:30:00.000Z]", e.getMessage());
        }
    }

    @Test
    public void testBadDateInInterval() {
        try {
            modelOf("timestamp = '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] invalid timestamp", e.getMessage());
        }
    }

    @Test
    public void testBadDateInIntervalVarchar() {
        try {
            modelOf("timestamp = '2014-0x-01T12:30:00.000Z'::varchar");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[38] Invalid date [str=2014-0x-01T12:30:00.000Z]", e.getMessage());
        }
    }

    @Test
    public void testBadDateVarchar() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.0001110z;30m'::varchar");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[46] Not a date, use IN keyword with intervals", e.getMessage());
        }
    }

    @Test
    public void testBadEndDate() {
        try {
            modelOf("timestamp in ('2014-01-02T12:30:00.000Z', '2014-01Z')");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertEquals("[42] Invalid date", e.getMessage());
        }
    }

    @Test
    public void testBadEndDateVarchar() {
        try {
            modelOf("timestamp in ('2014-01-02T12:30:00.000Z'::varchar, '2014-01Z'::varchar)");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertEquals("[61] Invalid date [str=2014-01Z]", e.getMessage());
        }
    }


    @Test
    public void testBadEpochInLess() {
        try {
            modelOf("'1663676011000000' < timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            Assert.assertEquals("[0] Invalid date [str='1663676011000000']", e.getMessage());
        }
    }

    @Test
    public void testBadEpochInLess2() {
        try {
            modelOf("timestamp < '1663676011000000'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[12] Invalid date [str='1663676011000000']", e.getMessage());
        }
    }

    @Test
    public void testBadEpochInLess2Varchar() {
        try {
            modelOf("timestamp < '1663676011000000'::varchar");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[30] Invalid date [str=1663676011000000]", e.getMessage());
        }
    }

    @Test
    public void testBadEpochInLessVarchar() {
        try {
            modelOf("'1663676011000000'::varchar < timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(18, e.getPosition());
            Assert.assertEquals("[18] Invalid date [str=1663676011000000]", e.getMessage());
        }
    }

    @Test
    public void testBadEqualsEpoch() {
        try {
            modelOf("timestamp = '1583077401000000'");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid timestamp");
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadEqualsEpochVarchar() {
        try {
            modelOf("timestamp = '1583077401000000'::varchar");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(30, e.getPosition());
        }
    }

    @Test
    public void testBadNotEqualsEpoch() {
        try {
            modelOf("timestamp != '1583077401000000'");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(13, e.getPosition());
        }
    }

    @Test
    public void testBadNotEqualsEpochVarchar() {
        try {
            modelOf("timestamp != '1583077401000000'::varchar");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(31, e.getPosition());
        }
    }

    @Test
    public void testBadOperators() {
        testBadOperator(">", "too few arguments for '>' [found=1,expected=2]");
        testBadOperator(">=", "too few arguments for '>=' [found=1,expected=2]");
        testBadOperator("<", "too few arguments for '<' [found=1,expected=2]");
        testBadOperator("<=", "too few arguments for '<=' [found=1,expected=2]");
        testBadOperator("=", "too few arguments for '=' [found=1,expected=2]");
        testBadOperator("!=", "too few arguments for '!=' [found=1,expected=2]");
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
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testBadStartDateVarchar() {
        try {
            modelOf("timestamp in ('2014-01Z'::varchar, '2014-01-02T12:30:00.000Z')");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(24, e.getPosition());
        }
    }

    @Test
    public void testBetweenFuncArgument() throws Exception {
        IntrinsicModel m = modelOf("dateadd(1, 'd', timestamp) between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp 'd' 1 dateadd between");
    }

    @Test
    public void testBetweenFuncArgumentVarchar() throws Exception {
        IntrinsicModel m = modelOf("dateadd(1, 'd', timestamp) between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "varchar '2014-01-02T12:30:00.000Z' cast varchar '2014-01-01T12:30:00.000Z' cast timestamp 'd' 1 dateadd between");
    }

    @Test
    public void testBetweenINowAndOneDayBefore() throws SqlException, NumericException {
        setCurrentMicros(MicrosTimestampDriver.floor("2014-01-03T12:30:00.000000Z"));
        runWhereTest("timestamp between now() and dateadd('d', -1, now())",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-03T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenInFunctionOfThreeArgs() throws Exception {
        IntrinsicModel m = modelOf("func(2, timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z', 'abc')");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'abc' '2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp between 2 func");
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
    public void testBetweenInFunctionOfThreeArgsDanglingVarchar() {
        try {
            modelOf("func(2, timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar,)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(102, e.getPosition());
            TestUtils.assertEquals("missing arguments", e.getFlyweightMessage());
        }
    }


    @Test
    public void testBetweenIntervalWithCaseStatementAsParam() throws SqlException {
        runWhereTest("timestamp between case when true then '2014-01-04T12:30:00.000Z' else '2014-01-02T12:30:00.000Z' end and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParam2() throws SqlException {
        runWhereTest("timestamp between " +
                        "'2014-01-02T12:30:00.000Z' " +
                        "and " +
                        "case when true then '2014-01-02T12:30:00.000Z' else '2014-01-03T12:30:00.000Z' end",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParam2Varchar() throws SqlException {
        runWhereTest("timestamp between " +
                        "'2014-01-02T12:30:00.000Z'::varchar " +
                        "and " +
                        "case when true then '2014-01-02T12:30:00.000Z'::varchar else '2014-01-03T12:30:00.000Z'::varchar end",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParamVarchar() throws SqlException {
        runWhereTest("timestamp between case when true then '2014-01-04T12:30:00.000Z'::varchar else '2014-01-02T12:30:00.000Z'::varchar end and '2014-01-02T12:30:00.000Z'::varchar",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParamWIthAndInCase() throws SqlException {
        runWhereTest("timestamp between case when true and true then '2014-01-04T12:30:00.000Z' else '2014-01-02T12:30:00.000Z' end and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenIntervalWithCaseStatementAsParamWIthAndInCaseVarchar() throws SqlException {
        runWhereTest("timestamp between case when true and true then '2014-01-04T12:30:00.000Z'::varchar else '2014-01-02T12:30:00.000Z'::varchar end and '2014-01-02T12:30:00.000Z'::varchar",
                "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
    }

    @Test
    public void testBetweenInvalidColumn() {
        try {
            modelOf("invalidTimestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unclosed quoted string?");
        }
    }

    @Test
    public void testBetweenWithDanglingCase() {
        try {
            runWhereTest("timestamp between case when true then '2014-01-04T12:30:00.000Z' else '2014-01-02T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                    "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(18, e.getPosition());
            TestUtils.assertEquals("unbalanced 'case'", e.getFlyweightMessage());
        }
    }

    @Test
    public void testBetweenWithDanglingCaseVarchar() {
        try {
            runWhereTest("timestamp between case when true then '2014-01-04T12:30:00.000Z'::varchar else '2014-01-02T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar",
                    "[{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-04T12:30:00.000000Z}]");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(18, e.getPosition());
            TestUtils.assertEquals("unbalanced 'case'", e.getFlyweightMessage());
        }
    }

    @Test
    public void testComplexInterval1() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00;2d'", "[{lo=2015-02-23T10:00:00.000000Z, hi=2015-02-25T10:00:59.999999Z}]");
    }

    @Test
    public void testComplexInterval1Varchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00;2d'::varchar", "[{lo=2015-02-23T10:00:00.000000Z, hi=2015-02-25T10:00:59.999999Z}]");
    }

    @Test
    public void testComplexInterval2() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;7d'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-03-02T10:00:55.000000Z}]");
    }

    @Test
    public void testComplexInterval2Varchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;7d'::varchar", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-03-02T10:00:55.000000Z}]");
    }

    @Test
    public void testComplexInterval3() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;15s'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}]");
    }

    @Test
    public void testComplexInterval3Varchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;15s'::varchar", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}]");
    }

    @Test
    public void testComplexInterval4() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;30m'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z}]");
    }

    @Test
    public void testComplexInterval4Varchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;30m'::varchar", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z}]");
    }

    @Test
    public void testComplexInterval5() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;30m' and timestamp != '2015-02-23T10:10:00.000Z'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:09:59.999999Z},{lo=2015-02-23T10:10:00.000001Z, hi=2015-02-23T10:30:55.000000Z}]");
    }

    @Test
    public void testComplexInterval5Varchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23T10:00:55.000Z;30m'::varchar and timestamp != '2015-02-23T10:10:00.000Z'::varchar", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:09:59.999999Z},{lo=2015-02-23T10:10:00.000001Z, hi=2015-02-23T10:30:55.000000Z}]");
    }

    @Test
    public void testComplexNow() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0(
                "timestamp < now() and timestamp > '1970-01-01T00:00:00.000Z'",
                "[{lo=1970-01-01T00:00:00.000001Z, hi=1970-01-01T23:59:59.999999Z}]");
    }

    @Test
    public void testComplexNowVarchar() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0(
                "timestamp < now() and timestamp > '1970-01-01T00:00:00.000Z'::varchar",
                "[{lo=1970-01-01T00:00:00.000001Z, hi=1970-01-01T23:59:59.999999Z}]");
    }

    @Test
    public void testComplexNowWithInclusive() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("now() >= timestamp and '1970-01-01T00:00:00.000Z' <= timestamp", "[{lo=1970-01-01T00:00:00.000000Z, hi=1970-01-02T00:00:00.000000Z}]");
    }

    @Test
    public void testComplexNowWithInclusiveVarchar() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("now() >= timestamp and '1970-01-01T00:00:00.000Z'::varchar <= timestamp", "[{lo=1970-01-01T00:00:00.000000Z, hi=1970-01-02T00:00:00.000000Z}]");
    }

    @Test
    public void testConstVsLambda() throws Exception {
        runWhereSymbolTest("sym in (1, 2) and ex in (select * from xyz)", "sym in (1, 2)");
    }

    @Test
    public void testConstVsLambda2() throws Exception {
        runWhereSymbolTest("ex in (1, 2) and ex in (select * from xyz)", "ex in (1, 2)");
    }

    @Test
    public void testContradictingNullSearch() throws Exception {
        IntrinsicModel m = modelOf("ex = null and ex != null and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym !=");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch10() throws Exception {
        IntrinsicModel m = modelOf("ex = null and ex != null and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym =");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch11() throws Exception {
        IntrinsicModel m = modelOf("ex = null and null != ex and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym =");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch2() throws Exception {
        IntrinsicModel m = modelOf("null = ex and null != ex and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym !=");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch3a() throws Exception {
        IntrinsicModel m = modelOf("sym = null and ex = 'blah' and sym != null");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "null sym != null sym = and");
        Assert.assertEquals("[blah]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch3b() throws Exception {
        IntrinsicModel m = modelOf("ex = null and sym = 'blah' and ex != null");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym =");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingNullSearch4() throws Exception {
        IntrinsicModel m = modelOf("ex != null and ex = null and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym !=");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testContradictingSearch1() throws Exception {
        IntrinsicModel m = modelOf("sym != 'blah' and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testContradictingSearch12() throws Exception {
        IntrinsicModel m = modelOf("sym != 'ho' and sym in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[null]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch13() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and not sym in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch14() throws Exception {
        IntrinsicModel m = modelOf("ex = 'ho' and not sym in ('blah') and not ex in (null, 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah' sym in not");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'blah' and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch3() throws Exception {
        IntrinsicModel m = modelOf("sym != 'blah' and sym in ('blah')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testContradictingSearch4() throws Exception {
        IntrinsicModel m = modelOf("sym in ('blah') and sym != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch5() throws Exception {
        IntrinsicModel m = modelOf("not (sym in ('blah')) and sym = 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch6() throws Exception {
        IntrinsicModel m = modelOf("sym = 'blah' and not (sym in ('blah'))");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch7() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and sym != 'blah' and sym != 'ho'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch8() throws Exception {
        IntrinsicModel m = modelOf("sym = 'ho' and not sym in ('blah', 'ho')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testContradictingSearch9() throws Exception {
        IntrinsicModel m = modelOf("sym != 'ho' and sym in ('blah', 'ho')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[blah]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testDesTimestampGreaterAndLess() throws Exception {
        runWhereTest("timestamp > '2015-02-23' and timestamp < '2015-02-24'",
                "[{lo=2015-02-23T00:00:00.000001Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testDesTimestampGreaterAndLessOrEqual() throws Exception {
        runWhereTest("timestamp >= '2015-02-23' and timestamp <= '2015-02-24'",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-24T00:00:00.000000Z}]");
    }

    @Test
    public void testDesTimestampGreaterAndLessOrEqualVarchar() throws Exception {
        runWhereTest("timestamp >= '2015-02-23'::varchar and timestamp <= '2015-02-24'::varchar",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-24T00:00:00.000000Z}]");
    }

    @Test
    public void testDesTimestampGreaterAndLessVarchar() throws Exception {
        runWhereTest("timestamp > '2015-02-23'::varchar and timestamp < '2015-02-24'::varchar",
                "[{lo=2015-02-23T00:00:00.000001Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testDesTimestampWithEpochGreaterAndLess() throws Exception {
        runWhereTest("timestamp > 1424649600000000::timestamp and timestamp < 1424736000000000::timestamp",
                "[{lo=2015-02-23T00:00:00.000001Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testDesTimestampWithEpochGreaterAndLessOrEqual() throws Exception {
        runWhereTest("timestamp >= 1424649600000000::timestamp and timestamp <= 1424736000000000::timestamp",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-24T00:00:00.000000Z}]");
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
    public void testEqualsAndInIndexedSearchWithFunction1a() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym in ( 'ADE', 'BCD') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[ADE]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1b() throws Exception {
        IntrinsicModel m = modelOf("sym != replace('AB', 'B', 'D') and sym in ( 'AD', 'BC') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[BC]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1c() throws Exception {
        IntrinsicModel m = modelOf("sym != replace('ABC', 'BC', 'DE') and sym not in ( 'ADE', 'BCD') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[ADE,BCD]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1d() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym not in ( 'ADE', 'BCD') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1e() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym not in ( 'B' || 'CD') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[ADE]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1f() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym not in ( 'A' || 'DE') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction1g() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym not in ( 'A' || 'DE', 'F') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction2() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym in ( 'ADF') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction3a() throws Exception {
        IntrinsicModel m = modelOf("sym in replace('AB', 'B', 'D') and sym in ( 'AE') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction3b() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in replace('ABC', 'BC', 'DE') and sym in ($1) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'DE' 'BC' 'ABC' replace sym in");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction3c() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in replace('ABC', 'BC', 'DE') and sym not in ($1) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[ADE]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "$1 sym in not");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction3d() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym not in replace('ABC', 'BC', 'DE') and sym in ($1) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'DE' 'BC' 'ABC' replace sym in not");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction4a() throws Exception {
        IntrinsicModel m = modelOf("sym in (ex, mode) and sym = 'mode' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[mode]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "mode ex sym in");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction4b() throws Exception {
        IntrinsicModel m = modelOf("sym in ('ex', 'mode') and sym = mode ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[ex,mode]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "mode sym =");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction4c() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("mode", "m");
        IntrinsicModel m = modelOf("sym in ('ex', 'mode') and sym = :mode ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[m]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'mode' 'ex' sym in");
    }

    @Test
    public void testEqualsAndInIndexedSearchWithFunction4d() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("ex", "e");
        bindVariableService.setStr("mode", "m");
        IntrinsicModel m = modelOf("sym in (:ex, :mode) and sym in ('mode', 'ex') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[mode,ex]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, ":mode :ex sym in");
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable1() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = 'a' and sym != $1");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym !=");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable2() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1 and sym != 'a'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'a' sym !=");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable3a() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1 and sym != '$1'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'$1' sym !=");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable3b() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym != $1 and sym != '$1'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym !=");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable3c() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1 and sym = '$1'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym =");
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable3d() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym != $1 and sym = '$1'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym !=");
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariable4() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1 and sym != $1");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariableAnotherColumnInTheMiddle1() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = 'a' and ex = 'c' and sym != $1");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym != 'a' sym = and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBindVariableAnotherColumnInTheMiddle2() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1 and ex = 'c' and sym != 'a'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'a' sym != $1 sym = and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBothBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym = $1 and ex = 'c' and sym != $2");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 sym != $1 sym = and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBothBindVariablesAnotherColumnInTheMiddle() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym = $1 and ex = 'c' and sym != $2");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 sym != $1 sym = and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBothConstants() throws Exception {
        IntrinsicModel m = modelOf("sym = 'a' and sym != 'b'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
    }

    @Test
    public void testEqualsAndNotEqualsBothConstantsAnotherColumnInTheMiddle() throws Exception {
        IntrinsicModel m = modelOf("sym = 'a' and ex = 'c' and sym != 'b'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'b' sym != 'a' sym = and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsAndNotEqualsBothFunctionsWithBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym = concat($1, 'c') and sym != concat($2, 'c')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'c' $2 concat sym != 'c' $1 concat sym = and");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testEqualsBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym = $1");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsBindVariableVarchar() throws Exception {
        bindVariableService.clear();
        bindVariableService.setVarchar(0, new Utf8String("a"));
        IntrinsicModel m = modelOf("sym = $1");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        TestUtils.assertEquals(new Utf8String("[a]"), keyValueFuncsToUtf8Sequence(m.keyValueFuncs));
    }

    @Test
    public void testEqualsChoiceOfColumns() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'X' sym =");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[Y]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test // both indexed columns have the same number of symbols (0) but different capacities
    public void testEqualsChoiceOfColumns2() throws Exception {
        IntrinsicModel m = modelOf("mode = 'X' and ex = 'Y'");
        assertFilter(m, "'X' mode =");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[Y]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test // indexed columns have different number of symbols
    public void testEqualsChoiceOfColumns3() throws Exception {
        IntrinsicModel m = nonEmptyModelOf();
        assertFilter(m, "'Z' mode = 'Y' ex = and");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsEpochTimestamp() throws Exception {
        runWhereTest("timestamp = 1424649600000000::timestamp", "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]");
    }

    @Test
    public void testEqualsIndexedSearch() throws Exception {
        IntrinsicModel m = modelOf("sym ='X' and bid > 100.05");
        assertFilter(m, "100.05 bid >");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchVarchar() throws Exception {
        IntrinsicModel m = modelOf("sym ='X'::varchar and bid > 100.05");
        assertFilter(m, "100.05 bid >");
        TestUtils.assertEquals("sym", m.keyColumn);
        TestUtils.assertEquals(new Utf8String("[X]"), keyValueFuncsToUtf8Sequence(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchWithFunction1() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' || '1' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X1]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchWithFunction10() throws Exception {
        IntrinsicModel m = modelOf("sym ~ 'A.*' and sym like 'AB%' ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'AB%' sym like 'A.*' sym ~ and");
    }

    @Test
    public void testEqualsIndexedSearchWithFunction2() throws Exception {
        IntrinsicModel m = modelOf("sym = case when 1 = 0 then 'A' else 'B' end ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[B]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchWithFunction3() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE')");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[ADE]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchWithFunction4() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym = 'ADE'");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[ADE]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsIndexedSearchWithFunction5a() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym = 'ADE' || 'A' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsIndexedSearchWithFunction5b() throws Exception {
        IntrinsicModel m = modelOf("sym = replace('ABC', 'BC', 'DE') and sym != 'ADE' || '' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsIndexedSearchWithFunction6() throws Exception {
        IntrinsicModel m = modelOf("sym = sysdate()::String and sym = now()::string ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "string now cast sym = String sysdate cast sym = and");
    }

    @Test
    public void testEqualsIndexedSearchWithFunction7() throws Exception {
        IntrinsicModel m = modelOf("sym != replace('BC', 'C', 'E') and sym != 'A' || 'D' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[AD,BE]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
    }

    @Test
    public void testEqualsIndexedSearchWithFunction7a() throws Exception {
        IntrinsicModel m = modelOf("sym = sysdate()::String and sym != 'A' || 'D' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[AD]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "String sysdate cast sym =");
    }

    @Test
    public void testEqualsIndexedSearchWithFunction8() throws Exception {
        IntrinsicModel m = modelOf("sym != sysdate()::String and sym != 'A' || 'D' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[AD]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "String sysdate cast sym !=");
    }

    @Test
    public void testEqualsIndexedSearchWithFunction9() throws Exception {
        IntrinsicModel m = modelOf("sym = 'A' || 'D' and sym != replace( 'A' || 'D', 'X', 'Y') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
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
        assertFilter(m, "(select-choose * from (x)) x =");
    }

    @Test
    public void testEqualsLambdaR() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) = x");
        assertFilter(m, "x (select-choose * from (x)) =");
    }

    @Test
    public void testEqualsNull() throws Exception {
        IntrinsicModel m = modelOf("sym = null");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[null]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'");
        assertFilter(m, null);
        Assert.assertEquals("[y]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'y' and sym in ('x','y')");
        assertFilter(m, null);
        Assert.assertEquals("[y]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsOverlapWithIn3() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'", "ex");
        assertFilter(m, "'y' sym = 'y' 'x' sym in and");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testEqualsOverlapWithInVarchar() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x'::varchar,'y'::varchar) and sym = 'y'::varchar");
        assertFilter(m, null);
        TestUtils.assertEquals(new Utf8String("[y]"), keyValueFuncsToUtf8Sequence(m.keyValueFuncs));
    }

    @Test
    public void testEqualsTo2DatesInterval() throws Exception {
        runWhereTest("timestamp in '2015-02-23'",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testEqualsToDateInterval() throws Exception {
        runWhereTest("timestamp in '2015-02-23'",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testEqualsToDateIntervalVarchar() throws Exception {
        runWhereTest("timestamp in '2015-02-23'::varchar",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T23:59:59.999999Z}]");
    }

    @Test
    public void testEqualsToDateTimestamp() throws Exception {
        runWhereTest("timestamp = '2015-02-23'",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]");
    }

    @Test
    public void testEqualsToDateTimestampVarchar() throws Exception {
        runWhereTest("timestamp = '2015-02-23'::varchar",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]");
    }

    @Test
    public void testEqualsZeroOverlapEquals() throws Exception {
        IntrinsicModel m = modelOf("sym = 'x' and sym = 'y'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
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
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-10T15:03:10.000000Z, hi=2015-05-10T15:03:10.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testExactDateVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z'::varchar and timestamp < '2015-05-11T08:00:55.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-10T15:03:10.000000Z, hi=2015-05-10T15:03:10.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testExactDateVsInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, null);
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("bid > 100 and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
        assertFilter(m, "100 bid >");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("sym in ('a', 'b', 'c') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b,c]", keyValueFuncsToString(m.keyValueFuncs));
        assertFilter(m, null);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("sym in ('a') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        assertFilter(m, null);
    }

    @Test
    public void testFilterOrInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 or timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp in 100 bid > or");
    }

    @Test
    public void testGreaterNoOpFilter() throws Exception {
        IntrinsicModel m = modelOf("bid > bid");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testGreaterOrEqualsNoOpFilter() throws Exception {
        IntrinsicModel m = modelOf("bid >= bid");
        Assert.assertEquals(IntrinsicModel.TRUE, m.intrinsicValue);
    }

    @Test
    public void testGreaterThanLambda() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) > x");
        assertFilter(m, "x (select-choose * from (x)) >");
    }

    @Test
    public void testGreaterThanLambdaR() throws Exception {
        IntrinsicModel m = modelOf("y > (select * from x)");
        assertFilter(m, "(select-choose * from (x)) y >");
    }

    @Test
    public void testInAndNotInBindVariable1() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ('a') and sym not in ($1)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym in not");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable2() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ($1) and sym not in ('a')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'a' sym in not");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable3() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ('$1') and sym not in ('$1')");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        TestUtils.assertEquals("sym", m.keyColumn);
    }

    @Test
    public void testInAndNotInBindVariable4() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym in ($1,$2) and sym not in ($2)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 sym in not");
        Assert.assertEquals("[a,b]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable5() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        bindVariableService.setStr(2, "c");
        IntrinsicModel m = modelOf("sym in ($1,$2) and sym not in ($2,$3)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$3 $2 sym in not");
        Assert.assertEquals("[a,b]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable6a() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        bindVariableService.setStr(2, "c");
        IntrinsicModel m = modelOf("sym in ($1,$2) and sym in ($2,$3)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 $1 sym in");
        Assert.assertEquals("[b,c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable6b() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        bindVariableService.setStr(2, "c");
        IntrinsicModel m = modelOf("sym in ($1,$2,$1) and sym in ($2,$3,$2,$3)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 $2 $1 sym in");
        Assert.assertEquals("[b,c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable7a() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        bindVariableService.setStr(2, "c");
        IntrinsicModel m = modelOf("sym not in ($1,$2) and sym not in ($2,$3)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[b,c,a]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable7b() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        bindVariableService.setStr(2, "c");
        IntrinsicModel m = modelOf("sym not in ($1,$2) and sym not in ($2,$2,$3,$3)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[b,c,a]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test //bind variable value is unknown so it can't be merged with any other except the same in IN set
    public void testInAndNotInBindVariable8a() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ($1) and sym in ('$1')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "$1 sym in");
    }

    @Test
    public void testInAndNotInBindVariable8b() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ('$1') and sym in ($1)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'$1' sym in");//either concrete values or bind variables have to be double checked
    }

    @Test//bind variable value is unknown so it can't be merged with any other value except the same in not IN set
    public void testInAndNotInBindVariable8c() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym not in ('$1') and sym not in ($1)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "'$1' sym in not");
    }

    @Test
    public void testInAndNotInBindVariable8d() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ('$1') and sym not in ($1)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym in not");
        Assert.assertEquals("[$1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable8e() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ($1) and sym not in ('$1')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'$1' sym in not");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariable8f() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ($1) and sym not in ('$' || '1')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'1' '$' concat sym in not");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariableAnotherColumnInTheMiddle1() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ('a') and ex = 'c' and sym not in ($1)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$1 sym in not 'a' sym in and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBindVariableAnotherColumnInTheMiddle2() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        IntrinsicModel m = modelOf("sym in ($1) and ex = 'c' and sym not in ('a')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'a' sym in not $1 sym in and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBothBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym in ($1) and sym not in ($2)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 sym in not");
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBothBindVariablesAnotherColumnInTheMiddle() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym in ($1) and ex = 'c' and sym not in ($2)");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "$2 sym in not $1 sym in and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBothConstants() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and sym not in ('b')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, null);
        Assert.assertEquals("[a]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBothConstantsAnotherColumnInTheMiddle() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and ex = 'c' and sym not in ('b')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'b' sym in not 'a' sym in and");
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInAndNotInBothFunctionsWithBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "a");
        bindVariableService.setStr(1, "b");
        IntrinsicModel m = modelOf("sym in (concat($1, 'c')) and sym not in (concat($2, 'c'))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'c' $2 concat sym in not 'c' $1 concat sym in and");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testInEpochRawValue() throws Exception {
        runWhereTest("timestamp in 1424649600000000::timestamp",
                "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]");
    }

    @Test
    public void testInIndexedSearchWithFunction0() throws Exception {
        IntrinsicModel m = modelOf("sym in ( replace( 'AAA', 'A', 'B' ), 'A' || 'B' ) and sym in ('BC', replace('AB', 'C', 'D') ) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[AB]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction0a() throws Exception {
        IntrinsicModel m = modelOf("sym in ( replace( 'AAA', 'AA', 'B' ), 'AB' ) and sym in ('BC', replace('AB', 'C', 'D') ) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[AB]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction1() throws Exception {
        IntrinsicModel m = modelOf("sym in ( replace( 'AAA', 'A', 'B' ) ) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[BBB]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction10() throws Exception {
        IntrinsicModel m = modelOf("sym not in ( 'A' || 'B', 'A' || 'A') and sym != 'E' || 'F' ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[EF,AB,AA]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction2() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1', concat( 'X', '2') ) ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X1,X2]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction3() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1') and sym in (concat( 'X', '2'))  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction4a() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1') and sym not in (concat( 'X', '2'))  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction4b() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1', replace('X3', '3', '2')) and sym not in (concat( 'X', '2'))  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction4c() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1', replace('X3', '3', '2')) and sym not in (concat( 'X', '2'), 'X' || '1')  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[X2,X1]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction5() throws Exception {
        IntrinsicModel m = modelOf("sym in ( 'X' || '1') and sym not in (systimestamp()::string)  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X1]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string systimestamp cast sym in not");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction6() throws Exception {
        IntrinsicModel m = modelOf("sym not in ( 'X' || '5' || '0') and sym not in (sysdate()::string)  ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[X50]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string sysdate cast sym in not");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction7a() throws Exception {
        IntrinsicModel m = modelOf("sym not in ( now()::string) and sym not in (sysdate()::string)  ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string sysdate cast sym in not string now cast sym in not and");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction7b() throws Exception {
        IntrinsicModel m = modelOf("not( sym in ( now()::string)) and not (sym in (sysdate()::string)) ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string sysdate cast sym in not string now cast sym in not and");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction8a() throws Exception {
        IntrinsicModel m = modelOf("sym not in ( now()::string, sysdate()::string)  ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string sysdate cast string now cast sym in not");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction8b() throws Exception {
        IntrinsicModel m = modelOf("not (sym in ( now()::string, sysdate()::string)) ");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "string sysdate cast string now cast sym in not");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInIndexedSearchWithFunction9() throws Exception {
        IntrinsicModel m = modelOf("sym not in ( 'A' || 'B', 'C' || 'D') and sym not in (replace('CD', 'E' ,'F'), 'E' || 'F') ");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[CD,EF,AB]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testInNull() throws Exception {
        IntrinsicModel m = modelOf("sym in ('X', null, 'Y')");
        Assert.assertEquals("[X,null,Y]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals("sym", m.keyColumn);
    }

    @Test
    public void testInVsEqualInterval() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp IN '2014-01-01'",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T23:59:59.999999Z}]");
        assertFilter(m, null);
    }

    @Test
    public void testIndexedFieldTooFewArgs2() throws Exception {
        assertFilter(modelOf("sym in (x)"), "x sym in");
    }

    @Test
    public void testIndexedFieldTooFewArgs3() {
        try {
            modelOf("sym in ()");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too few arguments");
        }
    }

    @Test
    public void testInterval() throws Exception {
        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'",
                        "timestamp < '2022-03-25T10:00:00.000000Z'",
                        "timestamp > '2022-03-26T19:20:52.792Z'"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'",
                        "timestamp < '2022-03-25T10:00:00.000000Z'",
                        "timestamp > dateadd('d', -10, now())"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'",
                        "timestamp < '2022-03-25T10:00:00.000000Z'",
                        "timestamp > dateadd('d', -10, '2022-04-05T19:20:52.792Z')"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z' AND now()",
                        "timestamp BETWEEN now() AND '2022-03-23T08:00:00.000000Z'",
                        "timestamp IN ('2022-03-23')",
                        "timestamp > dateadd('d', 1,'2022-03-23T08:00:00.000000Z')"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z' AND '2022-03-25T10:00:00.000000Z'",
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z' AND now()",
                        "timestamp NOT IN ('2022-03-25')",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'",
                        "timestamp < '2022-04'",
                        "timestamp > '2022-05'"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z' AND '2022-03-25T10:00:00.000000Z'",
                        "timestamp NOT IN ('2022-03-25')",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'",
                        "timestamp < '2022-04'"
                },
                replaceTimestampSuffix("[1648022400000000,1648166399999999]")
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z' AND '2022-03-25T10:00:00.000000Z'",
                        "timestamp NOT IN ('2022-03-25')",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'",
                        "timestamp < '2022-04'",
                        "timestamp NOT BETWEEN '2022-03-23T08:00:00.000000Z' AND '2022-03-25T10:00:00.000000Z'"
                },
                "[]"
        );
    }

    @Test
    public void testIntervalCaseSensitivityBetween() throws Exception {
        runWhereIntervalTest0("TIMESTAMP BETWEEN '2013-01-01T13:30:00.000Z' AND '2014-01-01T14:40:00.000Z'",
                "[{lo=2013-01-01T13:30:00.000000Z, hi=2014-01-01T14:40:00.000000Z}]");
    }

    @Test
    public void testIntervalCaseSensitivityGreater1() throws Exception {
        runWhereIntervalTest0("TIMESTAMP > '2013-01-01T13:30:00.000Z'",
                "[{lo=2013-01-01T13:30:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testIntervalCaseSensitivityGreater2() throws Exception {
        runWhereIntervalTest0("'2014-01-01T14:30:00.000Z' > TIMESTAMP",
                "[{lo=, hi=2014-01-01T14:29:59.999999Z}]");
    }

    @Test
    public void testIntervalCaseSensitivityIn() throws Exception {
        runWhereIntervalTest0("TIMESTAMP in '2015-02-23T10:00;2d'",
                "[{lo=2015-02-23T10:00:00.000000Z, hi=2015-02-25T10:00:59.999999Z}]");
    }

    @Test
    public void testIntervalCaseSensitivityLess1() throws Exception {
        runWhereIntervalTest0("TIMESTAMP < '2013-01-01T13:30:00.000Z'",
                "[{lo=, hi=2013-01-01T13:29:59.999999Z}]");
    }

    @Test
    public void testIntervalCaseSensitivityLess2() throws Exception {
        runWhereIntervalTest0("'2014-01-01T13:30:00.000Z' < TIMESTAMP",
                "[{lo=2014-01-01T13:30:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testIntervalDoNotIntersect() throws Exception {
        // because of intervals being processed from right to left
        // code will try to intersect 'not equal' will already be existing positive interval
        // result must be zero-overlap and FALSE model
        IntrinsicModel m = modelOf("timestamp != '2015-05-11' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testIntervalGreater1() throws Exception {
        runWhereCompareToModelTest("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp > '2014-01-01T15:30:00.000Z'",
                "[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testIntervalGreater2() throws Exception {
        runWhereCompareToModelTest("timestamp > '2014-01-01T15:30:00.000Z' and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testIntervalGreater3() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp > column1",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
        assertFilter(m, "column1 timestamp >");
    }

    @Test
    public void testIntervalGreater4() throws Exception {
        IntrinsicModel m = runWhereCompareToModelTest("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and column1 > timestamp",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
        assertFilter(m, "timestamp column1 >");
    }

    @Test
    public void testIntervalGreater5() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp > '2014-01-01T15:30:00.000Z'");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-01-01T15:30:00.000Z' timestamp >");
    }

    @Test
    public void testIntervalGreaterOrEq1() throws Exception {
        runWhereCompareToModelTest("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp >= '2014-01-01T15:30:00.000Z'",
                "[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testIntervalGreaterOrEq2() throws Exception {
        runWhereCompareToModelTest("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'",
                "[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]");
    }

    @Test
    public void testIntervalInManyArgs() throws SqlException {
        runWhereIntervalTest0(
                "timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z', '2014-01-03T12:30:00.000Z')",
                "[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z}," +
                        "{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}," +
                        "{lo=2014-01-03T12:30:00.000000Z, hi=2014-01-03T12:30:00.000000Z}]"
        );
    }

    @Test
    public void testIntervalInNotFunction() throws SqlException {
        IntrinsicModel m = modelOf("dateadd(1, 'd', timestamp) in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp 'd' 1 dateadd in");
    }

    @Test
    public void testIntervalInWithFractions() throws SqlException {
        runWhereIntervalTest0(
                "timestamp in ('2014-01-01T12:30:00.1')",
                "[{lo=2014-01-01T12:30:00.100000Z, hi=2014-01-01T12:30:00.199999Z}]"
        );
    }

    @Test
    public void testIntervalLessNoTimestamp() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp < '2014-01-01T15:30:00.000Z'");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-01-01T15:30:00.000Z' timestamp <");
    }

    @Test
    public void testIntervalSourceDay() throws Exception {
        runWhereCompareToModelTest("timestamp IN '2015-02-23T10:00:55.000Z;30m;2d;5'",
                "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z}," +
                        "{lo=2015-02-25T10:00:55.000000Z, hi=2015-02-25T10:30:55.000000Z}," +
                        "{lo=2015-02-27T10:00:55.000000Z, hi=2015-02-27T10:30:55.000000Z}," +
                        "{lo=2015-03-01T10:00:55.000000Z, hi=2015-03-01T10:30:55.000000Z}," +
                        "{lo=2015-03-03T10:00:55.000000Z, hi=2015-03-03T10:30:55.000000Z}]");
    }

    @Test
    public void testIntervalSourceHour() throws Exception {
        runWhereCompareToModelTest("timestamp in '2015-02-23T10:00:55.000Z;10m;3h;10'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:10:55.000000Z}," +
                "{lo=2015-02-23T13:00:55.000000Z, hi=2015-02-23T13:10:55.000000Z}," +
                "{lo=2015-02-23T16:00:55.000000Z, hi=2015-02-23T16:10:55.000000Z}," +
                "{lo=2015-02-23T19:00:55.000000Z, hi=2015-02-23T19:10:55.000000Z}," +
                "{lo=2015-02-23T22:00:55.000000Z, hi=2015-02-23T22:10:55.000000Z}," +
                "{lo=2015-02-24T01:00:55.000000Z, hi=2015-02-24T01:10:55.000000Z}," +
                "{lo=2015-02-24T04:00:55.000000Z, hi=2015-02-24T04:10:55.000000Z}," +
                "{lo=2015-02-24T07:00:55.000000Z, hi=2015-02-24T07:10:55.000000Z}," +
                "{lo=2015-02-24T10:00:55.000000Z, hi=2015-02-24T10:10:55.000000Z}," +
                "{lo=2015-02-24T13:00:55.000000Z, hi=2015-02-24T13:10:55.000000Z}]");
    }

    @Test
    public void testIntervalSourceMin() throws Exception {
        runWhereCompareToModelTest("timestamp in '2015-02-23T10:00:55.000Z;15s;15m;5'",
                "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}," +
                        "{lo=2015-02-23T10:15:55.000000Z, hi=2015-02-23T10:16:10.000000Z}," +
                        "{lo=2015-02-23T10:30:55.000000Z, hi=2015-02-23T10:31:10.000000Z}," +
                        "{lo=2015-02-23T10:45:55.000000Z, hi=2015-02-23T10:46:10.000000Z}," +
                        "{lo=2015-02-23T11:00:55.000000Z, hi=2015-02-23T11:01:10.000000Z}]");
    }

    @Test
    public void testIntervalSourceMonth() throws Exception {
        runWhereCompareToModelTest("timestamp IN '2015-02-23T10:00:55.000Z;2h;2M;3'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T12:00:55.000000Z}," +
                "{lo=2015-04-23T10:00:55.000000Z, hi=2015-04-23T12:00:55.000000Z}," +
                "{lo=2015-06-23T10:00:55.000000Z, hi=2015-06-23T12:00:55.000000Z}]");
    }

    @Test
    public void testIntervalSourceSec() throws Exception {
        runWhereCompareToModelTest("timestamp IN '2015-02-23T10:00:55.000Z;5s;30s;5'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:00.000000Z}," +
                "{lo=2015-02-23T10:01:25.000000Z, hi=2015-02-23T10:01:30.000000Z}," +
                "{lo=2015-02-23T10:01:55.000000Z, hi=2015-02-23T10:02:00.000000Z}," +
                "{lo=2015-02-23T10:02:25.000000Z, hi=2015-02-23T10:02:30.000000Z}," +
                "{lo=2015-02-23T10:02:55.000000Z, hi=2015-02-23T10:03:00.000000Z}]");
    }

    @Test
    public void testIntervalSourceYear() throws Exception {
        runWhereCompareToModelTest("timestamp IN '2015-02-23T10:00:55.000Z;1d;1y;5'", "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-24T10:00:55.000000Z}," +
                "{lo=2016-02-23T10:00:55.000000Z, hi=2016-02-24T10:00:55.000000Z}," +
                "{lo=2017-02-23T10:00:55.000000Z, hi=2017-02-24T10:00:55.000000Z}," +
                "{lo=2018-02-23T10:00:55.000000Z, hi=2018-02-24T10:00:55.000000Z}," +
                "{lo=2019-02-23T10:00:55.000000Z, hi=2019-02-24T10:00:55.000000Z}]");
    }

    @Test
    public void testIntervalTooFewArgs() {
        try {
            modelOf("timestamp in [\"2014-01-01T12:30:00.000Z\"]");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "'[' is unexpected here");
        }
    }

    @Test
    public void testIntervalTooFewArgs2() {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too few arg");
        }
    }

    @Test
    public void testIntervalVarchar() throws Exception {
        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'::varchar",
                        "timestamp < '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp > '2022-03-26T19:20:52.792Z'::varchar"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'::varchar",
                        "timestamp < '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp > dateadd('d', -10, now())"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp >= '2022-03-23T08:00:00.000000Z'::varchar",
                        "timestamp < '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp > dateadd('d', -10, '2022-04-05T19:20:52.792Z'::varchar)"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND now()",
                        "timestamp BETWEEN now() AND '2022-03-23T08:00:00.000000Z'::varchar",
                        "timestamp IN ('2022-03-23'::varchar)",
                        "timestamp > dateadd('d', 1,'2022-03-23T08:00:00.000000Z'::varchar)"
                },
                "[]"
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND now()",
                        "timestamp NOT IN ('2022-03-25'::varchar)",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'::varchar",
                        "timestamp < '2022-04'::varchar",
                        "timestamp > '2022-05'::varchar"
                },
                "[]"
        );


        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp NOT IN ('2022-03-25'::varchar)",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'::varchar",
                        "timestamp < '2022-04'::varchar"
                },
                replaceTimestampSuffix("[1648022400000000,1648166399999999]")
        );

        andShuffleExpressionsTest(
                new String[]{
                        "timestamp BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND '2022-03-25T10:00:00.000000Z'::varchar",
                        "timestamp NOT IN ('2022-03-25'::varchar)",
                        "timestamp != now() - 15",
                        "timestamp > '2021-01'::varchar",
                        "timestamp < '2022-04'::varchar",
                        "timestamp NOT BETWEEN '2022-03-23T08:00:00.000000Z'::varchar AND '2022-03-25T10:00:00.000000Z'::varchar"
                },
                "[]"
        );
    }

    @Test
    public void testIntrinsicPickup() throws Exception {
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' and sym in ('A', 'B') or ex = 'D'"), "'D' ex = 'B' 'A' sym in '2014-06-20T13:25:00.000Z;10m;2d;4' timestamp = and or");
        assertFilter(modelOf("(timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' or ex = 'D') and sym in ('A', 'B')"), "'D' ex = '2014-06-20T13:25:00.000Z;10m;2d;4' timestamp = or");
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
        runWhereSymbolTest("ex in (select a from xyz) and sym in (1, 2)", "sym in (1, 2)");
    }

    @Test
    public void testLambdaVsLambda() throws Exception {
        runWhereSymbolTest("sym in (select * from abc) and ex in (select * from xyz)", "sym in (select-choose * from (abc))");
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
    public void testLessNoOpFilter() throws Exception {
        IntrinsicModel m = modelOf("bid < bid");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testLessNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("timestamp < x");
        Assert.assertFalse(m.hasIntervalFilters());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "x timestamp <");
    }

    @Test
    public void testLessNonConstant2() throws SqlException {
        IntrinsicModel m = modelOf("x < timestamp");
        Assert.assertFalse(m.hasIntervalFilters());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "timestamp x <");
    }

    @Test
    public void testLessOrEqualsNoOpFilter() throws Exception {
        IntrinsicModel m = modelOf("bid <= bid");
        Assert.assertEquals(IntrinsicModel.TRUE, m.intrinsicValue);
    }

    @Test
    public void testLessThanLambda() throws Exception {
        IntrinsicModel m = modelOf("(select * from x) < x");
        assertFilter(m, "x (select-choose * from (x)) <");
    }

    @Test
    public void testLessThanLambdaR() throws Exception {
        IntrinsicModel m = modelOf("z < (select * from x)");
        assertFilter(m, "(select-choose * from (x)) z <");
    }

    @Test
    public void testListOfValuesNegativeOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('c')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('c', 'd', 'e'))");
        Assert.assertEquals("[a,z]", keyValueFuncsToString(m.keyValueFuncs));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause2() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'd', 'e'))");
        assertFilter(m, null);
        Assert.assertEquals("[z]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'z', 'e'))");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesPositiveOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testListOfValuesPositiveOverlapQuoteIndifference() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testListOfValuesPositiveOverlapVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z'::varchar, '2014-01-02T12:30:00.000Z'::varchar) and sym in ('a', 'z') and sym in ('z')");
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testLiteralInInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', c)");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "c '2014-01-01T12:30:00.000Z' timestamp in");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a', z) and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z 'a' sym in");
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
    public void testLiteralNotInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("not sym in ('a', z) and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z 'a' sym in not");
    }

    @Test
    public void testManualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp < '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]"), intervalToString(m));
    }

    @Test
    public void testManualIntervalInverted() throws Exception {
        IntrinsicModel m = modelOf("'2014-01-02T12:30:00.000Z' > timestamp and '2014-01-01T15:30:00.000Z' <= timestamp ");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]"), intervalToString(m));
    }

    @Test
    public void testManualIntervalVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z'::varchar and timestamp < '2014-01-02T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]"), intervalToString(m));
    }

    @Test
    public void testMultipleAnds() throws Exception {
        IntrinsicModel m = modelOf("a > 10 and b > 20 and (c > 100 and d < 20 and bid = 30)");
        assertFilter(m, "30 bid = 20 d < 100 c > and and 20 b > 10 a > and and");
    }

    @Test
    public void testNestedFunctionTest() throws Exception {
        IntrinsicModel m = modelOf("substr(parse(x, 1, 3), 2, 4)");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "4 2 3 1 x parse substr");
    }

    @Test
    public void testNoIntrinsics() throws Exception {
        IntrinsicModel m = modelOf("a > 10 or b > 20");
        Assert.assertFalse(m.hasIntervalFilters());
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "20 b > 10 a > or");
    }

    @Test
    public void testNotEqualInvalidColumn() {
        try {
            modelOf("ex != null and abb != 'blah'");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            Assert.assertEquals(15, e.getPosition());
        }
    }

    @Test
    public void testNotEqualPreferredColumn() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'", "ex");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "null sym != null sym = and");
        Assert.assertEquals("ex", m.keyColumn.toString());
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[blah]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testNotEqualsDoesNotOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("ex in ('x','y') and ex != 'z' and sym != 'blah'");
        assertFilter(m, "'blah' sym !=");
        Assert.assertEquals("[x,y]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testNotEqualsEpochTimestamp() throws Exception {
        runWhereIntervalTest0("timestamp != 1583077401000000::timestamp", "[{lo=, hi=2020-03-01T15:43:20.999999Z},{lo=2020-03-01T15:43:21.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testNotEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("ex in ('x','y') and ex != 'y' and sym != 'blah'");
        assertFilter(m, "'blah' sym !=");
        Assert.assertEquals("ex", m.keyColumn.toString());
        Assert.assertEquals("[x]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testNotEqualsOverlapWithNotIn() throws Exception {
        IntrinsicModel m = modelOf("sym != 'y' and not sym in ('x','y')");
        assertFilter(m, null);
        Assert.assertEquals("[x,y]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testNotIn() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp not in '2014-01-01'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2013-12-31T23:59:59.999999Z},{lo=2014-01-02T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(m));
    }

    @Test
    public void testNotInIntervalIntersect() throws Exception {
        IntrinsicModel m = modelOf("timestamp not between '2015-05-11T15:00:00.000Z' and '2015-05-11T20:00:00.000Z' and timestamp in '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]"),
                intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testNotInIntervalIntersect2() throws Exception {
        IntrinsicModel m = modelOf("timestamp in '2015-05-11' and not (timestamp between '2015-05-11T15:00:00.000Z' and '2015-05-11T20:00:00.000Z')");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]"),
                intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testNotInIntervalIntersect3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in '2015-05-11' and not (timestamp between '2015-05-11T15:00:00.000Z' and '2015-05-11T20:00:00.000Z') and not (timestamp between '2015-05-11T12:00:00.000Z' and '2015-05-11T14:00:00.000Z'))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T11:59:59.999999Z},{lo=2015-05-11T14:00:00.000001Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]"),
                intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testNotInIntervalInvalidHi() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z', 'abc')) and timestamp in '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(48, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalInvalidLo() {
        try {
            modelOf("not (timestamp in  ('abc','2015-05-11T15:00:00.000Z')) and timestamp in '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(20, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("not (timestamp in  (x, 'abc')) and timestamp in '2015-05-11'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T23:59:59.999999Z}]"), intervalToString(m));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'abc' x timestamp in not");
    }

    @Test
    public void testNotInIntervalNonLiteral() throws SqlException {
        IntrinsicModel m = modelOf("not (timestamp() in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");

        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T00:00:00.000000Z}]"), intervalToString(m));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'2015-05-11T15:00:00.000Z' timestamp in not");
    }

    @Test
    public void testNotInIntervalTooFew() {
        try {
            modelOf("not (timestamp in ['2015-05-11T15:00:00.000Z']) and timestamp IN '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "'[' is unexpected here");
        }
    }

    @Test
    public void testNotInIntervalTooMany() throws SqlException {
        runWhereIntervalTest0("(timestamp not in  ('2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z')) and timestamp in '2015-05-11'",
                "[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T15:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]");
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
    public void testNotInLambdaVsConst() throws Exception {
        IntrinsicModel m = modelOf("not (sym in (select a from xyz)) and not (ex in (1,2))");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[1,2]", keyValueFuncsToString(m.keyExcludedValueFuncs));
        assertFilter(m, "(select-choose a from (xyz)) sym in not");
    }

    @Test
    public void testNotInTooFew() {
        try {
            modelOf("not (ex in  ()) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too few");
            Assert.assertEquals(8, e.getPosition());
        }
    }

    @Test
    public void testNotInVarchar() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp not in '2014-01-01'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2013-12-31T23:59:59.999999Z},{lo=2014-01-02T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(m));
    }

    @Test
    public void testNowWithNotIn() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("timestamp not between '2020-01-01T00:00:00.000000000Z' and '2020-01-31T23:59:59.999999999Z' and now() <= timestamp",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=2019-12-31T23:59:59.999999Z}," +
                        "{lo=2020-02-01T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testNowWithNotInVarchar() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("timestamp not between '2020-01-01T00:00:00.000000000Z'::varchar and '2020-01-31T23:59:59.999999999Z'::varchar and now() <= timestamp",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=2019-12-31T23:59:59.999999Z}," +
                        "{lo=2020-02-01T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testOr1() throws Exception {
        IntrinsicModel m = modelOf("(sym = 'X' or sym = 'Y') and bid > 10");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "10 bid > 'Y' sym = 'X' sym = or and");
    }

    @Test
    public void testOr2() throws Exception {
        IntrinsicModel m = modelOf("((sym != 'A' or 1=2 ) and sym = 'A') and bid < 10");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "10 bid < 2 1 = 'A' sym != or and");
        Assert.assertEquals("[A]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testOrNullSearch() throws Exception {
        IntrinsicModel m = modelOf("(sym = null or sym != null) and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "null sym != null sym = or");
        Assert.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
        Assert.assertEquals("[blah]", keyValueFuncsToString(m.keyExcludedValueFuncs));
    }

    @Test
    public void testOrNullSearch2() throws Exception {
        IntrinsicModel m = modelOf("(sym = null or sym != null) and ex = 'blah'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "null sym != null sym = or");
        Assert.assertEquals("[blah]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testPreferredColumn() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110 ask < 100 bid > 'b' 'a' sym in and and");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testPreferredColumn2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110 ask < 100 bid > 'b' 'a' sym in and and");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testPreferredColumn3() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110 ask < 100 bid > 'b' 'a' sym in and and");
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testPreferredColumnVarchar() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110 ask < 100 bid > 'b' 'a' sym in and and");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testSeeminglyLookingDynamicInterval() throws Exception {
        // not equivalent to: timestamp >= '2022-03-23T08:00:00.000000Z' AND timestamp < '2022-03-25T10:00:00.000000Z' AND timestamp > '2022-03-26T19:20:52.792Z'
        // because 'systimestamp' is neither constant/runtime-constant, so the latter AND is not intrinsic and thus is out of the intervals model
        String whereExpression = "timestamp >= '2022-03-23T08:00:00.000000Z' AND timestamp < '2022-03-25T10:00:00.000000Z' AND timestamp > dateadd('d', -10, systimestamp())";
        setCurrentMicros(1649186452792000L); // '2022-04-05T19:20:52.792Z'
        try (RuntimeIntrinsicIntervalModel intervalModel = modelOf(whereExpression).buildIntervalModel()) {
            LongList intervals = intervalModel.calculateIntervals(sqlExecutionContext);
            TestUtils.assertEquals(replaceTimestampSuffix("[1648022400000000,1648202399999999]"), intervals.toString());
        }
    }

    @Test
    public void testSimpleBetweenAndInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSimpleBetweenAndIntervalVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSimpleEpochBetweenAndInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp between 1424649600000000::timestamp and 1424649600000000::timestamp");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSimpleIntervalVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSimpleLambda() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz)");
        Assert.assertNotNull(m.keySubQuery);
    }

    @Test
    public void testSingleEpochInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (1388579400000000::timestamp, 1388665800000000::timestamp)");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z},{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z},{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testSingleQuoteIntervalVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z'::varchar, '2014-01-02T12:30:00.000Z'::varchar)");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z},{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z},{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testThreeIntrinsics2() throws Exception {
        IntrinsicModel m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and bid > 100 and ask < 110");
        assertFilter(m, "110 ask < 100 bid > 'b' 'a' sym in and and");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testThreeIntrinsics2Varchar() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c'::varchar) and sym in ('a'::varchar, 'b'::varchar) and timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and bid > 100 and ask < 110");
        assertFilter(m, "110 ask < 100 bid > varchar 'b' cast varchar 'a' cast sym in and and");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testThreeIntrinsicsVarchar() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z'::varchar, '2014-01-02T12:30:00.000Z'::varchar) and bid > 100 and ask < 110");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", keyValueFuncsToString(m.keyValueFuncs));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T12:30:00.000000Z},{lo=2014-01-02T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testTimestampEpochEqualsLongConst() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);

        long ts = 1424649600000000L;
        if (timestampType == TestTimestampType.NANO) {
            ts = ts * 1000;
        }
        try {
            runWhereCompareToModelTest("timestamp = " + ts + " * 1",
                    "[{lo=2015-02-23T00:00:00.000000Z, hi=2015-02-23T00:00:00.000000Z}]");
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testTimestampEqualsConstFunction() throws Exception {
        runWhereCompareToModelTest("timestamp = to_date('2020-03-01:15:43:21', 'yyyy-MM-dd:HH:mm:ss')",
                "[{lo=2020-03-01T15:43:21.000000Z, hi=2020-03-01T15:43:21.000000Z}]");
    }

    @Test
    public void testTimestampEqualsConstFunctionVarchar() throws Exception {
        runWhereCompareToModelTest("timestamp = to_date('2020-03-01:15:43:21'::varchar, 'yyyy-MM-dd:HH:mm:ss')",
                "[{lo=2020-03-01T15:43:21.000000Z, hi=2020-03-01T15:43:21.000000Z}]");
    }

    @Test
    public void testTimestampEqualsFunctionOfNow() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereCompareToModelTest("timestamp = dateadd('d', 2, now())",
                "[{lo=1970-01-04T00:00:00.000000Z, hi=1970-01-04T00:00:00.000000Z}]");
    }

    @Test
    public void testTimestampEqualsFunctionOfNowVarchar() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereCompareToModelTest("timestamp = dateadd('d'::varchar, 2, now())",
                "[{lo=1970-01-04T00:00:00.000000Z, hi=1970-01-04T00:00:00.000000Z}]");
    }

    @Test
    public void testTimestampEqualsNow() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereCompareToModelTest("timestamp = now()",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=1970-01-02T00:00:00.000000Z}]");
    }

    @Test
    public void testTimestampEqualsNowAndSymbolsInList() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        IntrinsicModel m = runWhereCompareToModelTest("timestamp = now() and sym in (1, 2, 3)",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=1970-01-02T00:00:00.000000Z}]");
        assertFilter(m, null);
    }

    @Test
    public void testTimestampEqualsToBindVariable() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        runWhereIntervalTest0("timestamp = $1",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=1970-01-02T00:00:00.000000Z}]",
                bv -> bv.setTimestamp(0, day));
    }

    @Test
    public void testTimestampEqualsToConstNullFunc() throws SqlException {
        final long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        IntrinsicModel m = runWhereIntervalTest0("timestamp = to_date('2015-02-AB', 'yyyy-MM-dd')", "[]");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTimestampEqualsToNonConst() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        runWhereIntervalTest0("timestamp = dateadd('y',1,timestamp)", "");
    }

    @Test
    public void testTimestampEqualsToNonConstVarchar() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        runWhereIntervalTest0("timestamp = dateadd('y'::varchar,1,timestamp)", "");
    }

    @Test
    public void testTimestampFollowedByIntrinsicOperatorWithNull0() throws SqlException {
        assertFilter(modelOf("timestamp = null"), "null timestamp =");
        assertFilter(modelOf("timestamp != null"), "null timestamp !=");
        assertInterval(modelOf("timestamp in (null)"), "[{lo=, hi=}]");
        assertInterval(modelOf("timestamp in (null, null)"), "[{lo=, hi=}]");
        assertInterval(modelOf("timestamp not in (null)"), "[{lo=-290308-01-01T19:59:05.224193Z, hi=294247-01-10T04:00:54.775807Z}]");
        assertInterval(modelOf("timestamp not in (null, null)"), "[{lo=-290308-01-01T19:59:05.224193Z, hi=294247-01-10T04:00:54.775807Z}]");
        assertFilter(modelOf("timestamp >= null"), "null timestamp >=");
        assertFilter(modelOf("timestamp > null"), "null timestamp >");
        assertFilter(modelOf("timestamp <= null"), "null timestamp <=");
        assertFilter(modelOf("timestamp < null"), "null timestamp <");
        assertInterval(modelOf("timestamp between null and null"), "[]");
        assertInterval(modelOf("timestamp not between null and null"), "");
    }

    @Test
    public void testTimestampFollowedByIntrinsicOperatorWithNull1() throws SqlException {
        // in this case no designated timestamp column
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp = null"), "null timestamp =");
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp != null"), "null timestamp !=");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp in (null)"), "");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp in (null, null)"), "");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp not in (null)"), "");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp not in (null, null)"), "");
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp >= null"), "null timestamp >=");
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp > null"), "null timestamp >");
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp <= null"), "null timestamp <=");
        assertFilter(noDesignatedTimestampNotIdxModelOf("timestamp < null"), "null timestamp <");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp between null and null"), "");
        assertInterval(noDesignatedTimestampNotIdxModelOf("timestamp not between null and null"), "");
    }

    @Test
    public void testTimestampFunctionOfThreeArgs() throws Exception {
        IntrinsicModel m = modelOf("func(2, timestamp, 'abc')");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'abc' timestamp 2 func");
    }

    @Test
    public void testTimestampFunctionOfThreeArgsVarchar() throws Exception {
        IntrinsicModel m = modelOf("func(2, timestamp, 'abc'::varchar)");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "varchar 'abc' cast timestamp 2 func");
    }

    @Test
    public void testTimestampGreaterConstFunction() throws SqlException {
        runWhereIntervalTest0("timestamp > to_date('2015-02-22', 'yyyy-MM-dd')", "[{lo=2015-02-22T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampGreaterConstFunctionVarchar() throws SqlException {
        runWhereIntervalTest0("timestamp > to_date('2015-02-22'::varchar, 'yyyy-MM-dd'::varchar)", "[{lo=2015-02-22T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampLessConstFunction() throws SqlException {
        runWhereIntervalTest0("timestamp <= to_date('2015-02-22', 'yyyy-MM-dd')", "[{lo=, hi=2015-02-22T00:00:00.000000Z}]");
    }

    @Test
    public void testTimestampLessConstFunctionVarchar() throws SqlException {
        runWhereIntervalTest0("timestamp <= to_date('2015-02-22'::varchar, 'yyyy-MM-dd'::varchar)", "[{lo=, hi=2015-02-22T00:00:00.000000Z}]");
    }

    @Test
    public void testTimestampNotEqualsConstFunction() throws Exception {
        runWhereIntervalTest0("timestamp != to_date('2020-03-01:15:43:21', 'yyyy-MM-dd:HH:mm:ss')",
                "[{lo=, hi=2020-03-01T15:43:20.999999Z},{lo=2020-03-01T15:43:21.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampNotEqualsConstFunctionVarchar() throws Exception {
        runWhereIntervalTest0("timestamp != to_date('2020-03-01:15:43:21'::varchar, 'yyyy-MM-dd:HH:mm:ss'::varchar)",
                "[{lo=, hi=2020-03-01T15:43:20.999999Z},{lo=2020-03-01T15:43:21.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampNotEqualsFunctionOfNow() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("timestamp != dateadd('d', 2, now())",
                "[{lo=, hi=1970-01-03T23:59:59.999999Z},{lo=1970-01-04T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampNotEqualsNow() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        runWhereIntervalTest0("timestamp != now()",
                "[{lo=, hi=1970-01-01T23:59:59.999999Z},{lo=1970-01-02T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
    }

    @Test
    public void testTimestampNotEqualsNowAndSymbolsNotInList() throws Exception {
        setCurrentMicros(24L * 3600 * 1000 * 1000);
        IntrinsicModel m = runWhereIntervalTest0("timestamp != now() and sym not in (1, 2, 3)",
                "[{lo=, hi=1970-01-01T23:59:59.999999Z},{lo=1970-01-02T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]");
        assertFilter(m, null);
    }

    @Test
    public void testTimestampNotEqualsToBindVariable() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        runWhereIntervalTest0("timestamp != $1",
                "[{lo=, hi=1970-01-01T23:59:59.999999Z},{lo=1970-01-02T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]",
                bv -> bv.setTimestamp(0, day));
    }

    @Test
    public void testTimestampNotEqualsToNonConst() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, day);
        runWhereIntervalTest0("timestamp != dateadd('y',1,timestamp)", "");
    }

    @Test
    public void testTimestampWithBindNullVariable() throws SqlException {
        runWhereIntervalTest0("timestamp >= $1", "[]", bv -> bv.setTimestamp(0, Numbers.LONG_NULL));
    }

    @Test
    public void testTimestampWithBindVariable() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        runWhereIntervalTest0("timestamp >= $1",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]",
                bv -> bv.setTimestamp(0, day));
    }

    @Test
    public void testTimestampWithBindVariableCombinedNot() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        runWhereIntervalTest0("timestamp != $1 and timestamp != $2",
                "[{lo=, hi=1970-01-01T23:59:59.999999Z},{lo=1970-01-02T00:00:00.000001Z, hi=1970-01-02T23:59:59.999999Z},{lo=1970-01-03T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]",
                bv -> {
                    bv.setTimestamp(0, day);
                    bv.setTimestamp(1, 2 * day);
                });
    }

    @Test
    public void testTimestampWithBindVariableWithin() throws SqlException {
        long day = 24L * 3600 * 1000 * 1000;
        runWhereCompareToModelTest("timestamp >= $1 and timestamp <= $2",
                "[{lo=1970-01-02T00:00:00.000000Z, hi=1970-01-03T00:00:00.000000Z}]",
                bv -> {
                    bv.setTimestamp(0, day);
                    bv.setTimestamp(1, 2 * day);
                });
    }

    @Test
    public void testTwoBetweenIntervalsForDoubleColumn() throws Exception {
        IntrinsicModel m = modelOf("bid between 5 and 10 ");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "10 5 bid between");
    }

    @Test
    public void testTwoBetweenIntervalsForExpression() throws SqlException {
        IntrinsicModel m = modelOf("ask between bid+ask/2 and 10 ");
        assertFilter(m, "10 2 ask / bid + ask between");
    }

    @Test
    public void testTwoBetweenIntervalsForExpression2() throws SqlException {
        IntrinsicModel m = modelOf("ask between 1 and bid+ask/2");
        assertFilter(m, "2 ask / bid + 1 ask between");
    }

    @Test
    public void testTwoBetweenIntervalsForIntColumn() throws Exception {
        IntrinsicModel m = modelOf("bidSize between 5 and 10 ");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "10 5 bidSize between");
    }

    @Test
    public void testTwoBetweenIntervalsWithAnd() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp between '2014-01-01T16:30:00.000Z' and '2014-01-05T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testTwoBetweenIntervalsWithAndVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and timestamp between '2014-01-01T16:30:00.000Z'::varchar and '2014-01-05T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testTwoBetweenIntervalsWithOr() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' or timestamp between '2014-02-01T12:30:00.000Z' and '2014-02-02T12:30:00.000Z'");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-02-02T12:30:00.000Z' '2014-02-01T12:30:00.000Z' timestamp between '2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp between or");
    }

    @Test
    public void testTwoBetweenIntervalsWithOrVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar or timestamp between '2014-02-01T12:30:00.000Z'::varchar and '2014-02-02T12:30:00.000Z'::varchar");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "varchar '2014-02-02T12:30:00.000Z' cast varchar '2014-02-01T12:30:00.000Z' cast timestamp between varchar '2014-01-02T12:30:00.000Z' cast varchar '2014-01-01T12:30:00.000Z' cast timestamp between or");
    }

    @Test
    public void testTwoDiffColLambdas() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz) and ex in (select  * from kkk)");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals(ExpressionNode.QUERY, m.filter.rhs.type);
    }

    @Test
    public void testTwoExactMatchDifferentDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactMatchDifferentDatesVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z'::varchar and timestamp = '2015-05-11T15:03:10.000Z'::varchar and timestamp = '2015-05-11'::varchar");
        TestUtils.assertEquals("[]", intervalToString(m));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDatesVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z'::varchar and timestamp = '2015-05-10T15:03:10.000Z'::varchar and timestamp = '2015-05-11'::varchar");
        TestUtils.assertEquals("[]", intervalToString(m));
        assertFilter(m, null);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testTwoIntervalSources() throws Exception {
        IntrinsicModel m = modelOf("timestamp in '2014-06-20T13:25:00.000Z;10m;2d;5' and timestamp IN '2015-06-20T13:25:00.000Z;10m;2d;5'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        TestUtils.assertEquals("[]", intervalToString(m));
    }

    @Test
    public void testTwoIntervalSourcesVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp in '2014-06-20T13:25:00.000Z;10m;2d;5'::varchar and timestamp IN '2015-06-20T13:25:00.000Z;10m;2d;5'::varchar");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        TestUtils.assertEquals("[]", intervalToString(m));
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp between '2014-01-01T16:30:00.000Z' and '2014-01-05T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testTwoIntervalsVarchar() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and timestamp between '2014-01-01T16:30:00.000Z'::varchar and '2014-01-05T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testTwoIntervalsWithAnd() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z' and '2014-01-02T12:30:00.000Z' and timestamp between '2014-01-01T16:30:00.000Z' and '2014-01-05T12:30:00.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testTwoIntervalsWithAndVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp between '2014-01-01T12:30:00.000Z'::varchar and '2014-01-02T12:30:00.000Z'::varchar and timestamp between '2014-01-01T16:30:00.000Z'::varchar and '2014-01-05T12:30:00.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]"), intervalToString(m));
    }

    @Test
    public void testTwoIntervalsWithOr() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') or timestamp in ('2014-02-01T12:30:00.000Z', '2014-02-02T12:30:00.000Z')");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "'2014-02-02T12:30:00.000Z' '2014-02-01T12:30:00.000Z' timestamp in '2014-01-02T12:30:00.000Z' '2014-01-01T12:30:00.000Z' timestamp in or");
    }

    @Test
    public void testTwoIntervalsWithOrVarchar() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z'::varchar, '2014-01-02T12:30:00.000Z'::varchar) or timestamp in ('2014-02-01T12:30:00.000Z'::varchar, '2014-02-02T12:30:00.000Z'::varchar)");
        Assert.assertFalse(m.hasIntervalFilters());
        assertFilter(m, "varchar '2014-02-02T12:30:00.000Z' cast varchar '2014-02-01T12:30:00.000Z' cast timestamp in varchar '2014-01-02T12:30:00.000Z' cast varchar '2014-01-01T12:30:00.000Z' cast timestamp in or");
    }

    @Test
    public void testTwoNestedBetween1() {
        try {
            modelOf("ask between between 1 and 2 and bid+ask/2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between statements cannot be nested");
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testTwoNestedBetween2() {
        try {
            modelOf("ask between (between 1 and 2) and bid+ask/2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "between statements cannot be nested");
            Assert.assertEquals(13, e.getPosition());
        }
    }

    @Test
    public void testTwoNot() throws SqlException {
        IntrinsicModel m = modelOf("timestamp != '2015-05-10T15:03:10.000Z' and timestamp != '2015-05-10T16:03:10.000Z'");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2015-05-10T15:03:09.999999Z},{lo=2015-05-10T15:03:10.000001Z, hi=2015-05-10T16:03:09.999999Z},{lo=2015-05-10T16:03:10.000001Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testTwoNotVarchar() throws SqlException {
        IntrinsicModel m = modelOf("timestamp != '2015-05-10T15:03:10.000Z'::varchar and timestamp != '2015-05-10T16:03:10.000Z'::varchar");
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2015-05-10T15:03:09.999999Z},{lo=2015-05-10T15:03:10.000001Z, hi=2015-05-10T16:03:09.999999Z},{lo=2015-05-10T16:03:10.000001Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(m));
        assertFilter(m, null);
    }

    @Test
    public void testTwoSameColLambdas() {
        try {
            modelOf("sym in (select * from xyz) and sym in (select * from kkk)");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "Multiple lambda");
        }
    }

    @Test
    public void testUnindexedEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym = 'ABC'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedEqualsVarchar() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'::varchar", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym = 'ABC'::varchar", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1, 2)", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym in (1, 2)", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedPreferredEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        assertFilter(m, null);
        TestUtils.assertEquals("[ABC]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedPreferredEqualsVarchar() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'::varchar", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        assertFilter(m, null);
        TestUtils.assertEquals("[ABC]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedPreferredIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2)", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        assertFilter(m, null);
        TestUtils.assertEquals("[1,2]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedPreferredInVsIndexed() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2) and ex in ('XYZ')", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        TestUtils.assertEquals("ex in 'XYZ'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[1,2]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testUnindexedPreferredInVsIndexedVarchar() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2) and ex in ('XYZ'::varchar)", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        TestUtils.assertEquals("ex in 'XYZ'::varchar", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[1,2]", keyValueFuncsToString(m.keyValueFuncs));
    }

    @Test
    public void testVarcharPracticalParsing() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testVarcharPracticalParsing_" + timestampType.getTypeName();
            executeWithRewriteTimestamp("create table " + tableName + " ( a string, ts #TIMESTAMP) timestamp(ts)", timestampType.getTypeName());
            assertPlanNoLeakCheck(
                    "select * from " + tableName + " where\n" +
                            "ts = '2024-02-29' or ts <= '2024-03-01'",
                    "Async JIT Filter workers: 1\n" +
                            "  filter: (ts=2024-02-29T00:00:00.000000Z or 2024-03-01T00:00:00.000000Z>=ts) [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: " + tableName + "\n"
            );

            assertPlanNoLeakCheck(
                    "select * from " + tableName + " where\n" +
                            "(ts = '2024-02-29'::varchar or ts <= '2024-03-01'::varchar) or ts = '2024-05-01'::varchar",
                    "Async Filter workers: 1\n" +
                            "  filter: ((ts=2024-02-29T00:00:00.000000Z or 2024-03-01T00:00:00.000000Z>=ts) or ts=2024-05-01T00:00:00.000000Z) [pre-touch]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: " + tableName + "\n"
            );
        });
    }

    @Test
    public void testVarcharTimestampParseBasic() throws SqlException {
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000000Z, hi=2024-02-29T00:00:00.000000Z}]"), intervalToString(modelOf("timestamp = '2024-02-29'::varchar")));
    }

    @Test
    public void testVarcharTimestampParseCompoundExpr() throws SqlException {
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000000Z, hi=2024-02-29T00:00:00.000000Z}]"), intervalToString(modelOf("timestamp = '2024-02-29'::varchar and timestamp <= '2024-03-01'::varchar")));
    }

    @Test
    public void testVarcharTimestampParseOperators() throws SqlException {
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000000Z, hi=2024-02-29T00:00:00.000000Z}]"), intervalToString(modelOf("timestamp = '2024-02-29'::varchar")));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(modelOf("timestamp > '2024-02-29'::varchar")));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2024-02-28T23:59:59.999999Z}]"), intervalToString(modelOf("timestamp < '2024-02-29'::varchar")));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=, hi=2024-02-29T00:00:00.000000Z}]"), intervalToString(modelOf("timestamp <= '2024-02-29'::varchar")));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]"), intervalToString(modelOf("timestamp >= '2024-02-29'::varchar")));
        TestUtils.assertEquals(replaceTimestampSuffix("[{lo=2024-02-29T00:00:00.000000Z, hi=2024-03-01T00:00:00.000000Z}]"), intervalToString(modelOf("timestamp between '2024-02-29'::varchar and '2024-03-01'::varchar")));
    }

    @Test
    public void testWrongTypeConstFunctionDateGreater() {
        try {
            modelOf("timestamp > abs(1)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testWrongTypeConstFunctionDateLess() {
        try {
            modelOf("timestamp <= abs(1)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(13, e.getPosition());
        }
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private void andShuffleExpressionsTest(String[] expressions, String expected) throws SqlException {
        shuffleExpressionsTest(expressions, " AND ", expected, 0);
    }

    private void assertFilter(IntrinsicModel m, CharSequence expected) throws SqlException {
        if (expected == null) {
            if (m.filter != null) {
                Assert.assertNull("Filter", toRpn(m.filter));
            }
        } else {
            TestUtils.assertEquals("Filter", expected, m.filter != null ? toRpn(m.filter) : null);
        }
    }

    private void assertInterval(IntrinsicModel m, CharSequence expected) throws SqlException {
        TestUtils.assertEquals(replaceTimestampSuffix(expected.toString()), intervalToString(m));
    }

    private CharSequence intervalToString(IntrinsicModel model) throws SqlException {
        if (!model.hasIntervalFilters()) {
            return "";
        }
        try (RuntimeIntrinsicIntervalModel sm = model.buildIntervalModel()) {
            return GriffinParserTestUtils.intervalToString(timestampType.getDriver(), sm.calculateIntervals(sqlExecutionContext));
        }
    }

    private String keyValueFuncsToString(ObjList<Function> keyValueFuncs) {
        StringBuilder b = new StringBuilder();
        b.setLength(0);
        b.append('[');
        for (int i = 0, k = keyValueFuncs.size(); i < k; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(keyValueFuncs.getQuick(i).getStrA(null));
        }
        b.append(']');
        return b.toString();
    }

    private Utf8Sequence keyValueFuncsToUtf8Sequence(ObjList<Function> keyValueFuncs) {
        Utf8StringSink b = new Utf8StringSink();
        b.put('[');
        for (int i = 0, k = keyValueFuncs.size(); i < k; i++) {
            if (i > 0) {
                b.put(',');
            }
            b.put(keyValueFuncs.getQuick(i).getVarcharA(null));
        }
        b.put(']');
        return b;
    }

    private IntrinsicModel modelOf(CharSequence seq) throws SqlException {
        return modelOf(seq, null);
    }

    private IntrinsicModel modelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            RecordMetadata m = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? metadata : metadataNanos;
            return e.extract(
                    column -> column,
                    compiler.testParseExpression(seq, queryModel),
                    m,
                    preferredColumn,
                    m.getTimestampIndex(),
                    functionParser,
                    m,
                    sqlExecutionContext,
                    false,
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? reader : readerNanos
            );
        }
    }

    private IntrinsicModel noDesignatedTimestampNotIdxModelOf(CharSequence seq) throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            RecordMetadata m = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? noDesignatedTimestampNorIdxMetadata : noDesignatedTimestampNorIdxMetadataNanos;
            return e.extract(
                    column -> column,
                    compiler.testParseExpression(seq, queryModel),
                    m,
                    null,
                    m.getTimestampIndex(),
                    functionParser,
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? metadata : metadataNanos,
                    sqlExecutionContext,
                    false,
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? noDesignatedTimestampNorIdxReader : noDesignatedTimestampNorIdxReaderNanos
            );
        }
    }

    private IntrinsicModel noTimestampModelOf(CharSequence seq) throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            return e.extract(
                    column -> column,
                    compiler.testParseExpression(seq, queryModel),
                    noTimestampMetadata,
                    null,
                    noTimestampMetadata.getTimestampIndex(),
                    functionParser,
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? metadata : metadataNanos,
                    sqlExecutionContext,
                    false,
                    noTimestampReader
            );
        }
    }

    private IntrinsicModel nonEmptyModelOf() throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            RecordMetadata m = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? nonEmptyMetadata : nonEmptyMetadataNanos;
            return e.extract(
                    column -> column,
                    compiler.testParseExpression("sym = 'X' and ex = 'Y' and mode = 'Z'", queryModel),
                    m,
                    null,
                    m.getTimestampIndex(),
                    functionParser,
                    metadata,
                    sqlExecutionContext,
                    false,
                    ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? nonEmptyReader : nonEmptyReaderNanos
            );
        }
    }

    private String replaceTimestampSuffix(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? expected.replaceAll("00000", "00000000")
                .replaceAll("99999", "99999999")
                .replaceAll("294247-01-10T04:00:54.775807Z", "2262-04-11T23:47:16.854775807Z")
                .replaceAll("-290308-01-01T19:59:05.224193Z", "1677-01-01T00:12:43.145224193Z")
                : expected;
    }

    private IntrinsicModel runWhereCompareToModelTest(String where, String expected) throws SqlException {
        return runWhereCompareToModelTest(where, expected, null);
    }

    private IntrinsicModel runWhereCompareToModelTest(String where, String expected, SetBindVars bindVars) throws SqlException {
        runWhereIntervalTest0(where + " and timestamp < dateadd('y', 100, now())", expected, bindVars);
        runWhereIntervalTest0(where + " and dateadd('y', 100, now()) > timestamp", expected, bindVars);

        runWhereIntervalTest0("timestamp < dateadd('y', 100, now()) and " + where, expected, bindVars);
        runWhereIntervalTest0("dateadd('y', 100, now()) > timestamp and " + where, expected, bindVars);

        runWhereIntervalTest0(where + " and timestamp > dateadd('y', -100, now())", expected, bindVars);
        runWhereIntervalTest0(where + " and dateadd('y', -100, now()) < timestamp", expected, bindVars);

        runWhereIntervalTest0("timestamp > dateadd('y', -100, now()) and " + where, expected, bindVars);
        runWhereIntervalTest0("dateadd('y', -100, now()) < timestamp and " + where, expected, bindVars);

        return runWhereIntervalTest0(where, expected, bindVars);
    }

    private IntrinsicModel runWhereIntervalTest0(String where, String expected) throws SqlException {
        return runWhereIntervalTest0(where, expected, null);
    }

    private IntrinsicModel runWhereIntervalTest0(String where, String expected, SetBindVars bindVars) throws SqlException {
        bindVariableService.clear();
        IntrinsicModel m = modelOf(where);
        if (bindVars != null) {
            bindVars.set(bindVariableService);
        }

        TestUtils.assertEquals(replaceTimestampSuffix(expected), intervalToString(m));
        return m;
    }

    private void runWhereSymbolTest(String where, String toRpn) throws SqlException {
        IntrinsicModel m = modelOf(where);
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(toRpn, GriffinParserTestUtils.toRpn(m.filter));
    }

    private void runWhereTest(String where, String expected) throws SqlException {
        IntrinsicModel m = runWhereCompareToModelTest(where, expected);
        Assert.assertEquals("IntrinsicModel{keyValueFuncs=[], keyColumn='null', filter=null}", m.toString());
    }

    private void shuffleExpressionsTest(String[] expressions, String separator, String expected, int k) throws SqlException {
        for (int i = k; i < expressions.length; i++) {
            swap(expressions, i, k);
            shuffleExpressionsTest(expressions, separator, expected, k + 1);
            swap(expressions, k, i);
        }
        if (k == expressions.length - 1) {
            sink.clear();
            for (String s : expressions) {
                sink.put(s).put(separator);
            }
            sink.clear(sink.length() - separator.length());
            String expression = sink.toString();
            try (RuntimeIntrinsicIntervalModel ignore = modelOf(expression).buildIntervalModel()) {
                Assert.assertEquals(
                        "shuffled expression '" + expression + "' has unexpected result",
                        expected,
                        ignore.calculateIntervals(sqlExecutionContext).toString()
                );
            }
        }
    }

    private void testBadOperator(String op, String expected) {
        try {
            modelOf("sum(ts) " + op);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[8] " + expected, e.getMessage());
        }

        try {
            modelOf(op + " sum(ts)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[0] " + expected, e.getMessage());
        }

    }

    //reverse polish notation?
    private CharSequence toRpn(ExpressionNode node) throws SqlException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }

    private IntrinsicModel unindexedModelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            RecordMetadata m = ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? unindexedMetadata : unindexedMetadataNanos;
            return e.extract(
                    column -> column,
                    compiler.testParseExpression(seq, queryModel),
                    m,
                    preferredColumn,
                    m.getTimestampIndex(),
                    functionParser,
                    metadata,
                    sqlExecutionContext,
                    false,
                    unindexedReader
            );
        }
    }

    @FunctionalInterface
    private interface SetBindVars {
        void set(BindVariableService bindVariableService) throws SqlException;
    }
}
