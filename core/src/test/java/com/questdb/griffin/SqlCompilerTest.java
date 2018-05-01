/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.Engine;
import com.questdb.cairo.SymbolMapReader;
import com.questdb.cairo.TableReader;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SqlCompilerTest extends AbstractCairoTest {
    private final static Engine engine = new Engine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();

    @After
    public void tearDown() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    @Test
    public void testCastIntLong() throws SqlException, IOException {
        String expectedData = "a\n" +
                "25\n" +
                "29\n" +
                "9\n" +
                "0\n" +
                "11\n" +
                "21\n" +
                "11\n" +
                "28\n" +
                "2\n" +
                "NaN\n" +
                "1\n" +
                "6\n" +
                "NaN\n" +
                "4\n" +
                "17\n" +
                "2\n" +
                "NaN\n" +
                "25\n" +
                "27\n" +
                "3\n";

        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"LONG\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 123," + // first 64 bits of random seed
                " 345," + // second 64 bits of random seed
                " 'a', rnd_int(0, 30, 2)" +
                ")), cast(a as LONG)";

        assertCast(expectedData, expectedMeta, sql);
    }

    @Test
    public void testCreateEmptyTableNoPartition() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t)",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.NONE, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableNoTimestamp() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":-1}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolCache() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolNoCache() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 nocache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertFalse(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableWithIndex() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache index capacity 2048, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2048},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateTableAsSelect() throws SqlException, IOException {
        String expectedData = "a1\ta\tb\tc\td\te\tf\tf1\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                "-363815248\t21\ttrue\tJBS\t0.451825191892\t0.9278\t607\t-2950\t2015-04-02T11:43:00.700Z\t2015-12-24T02:22:12.820Z\tRFOJ\tNaN\t-6636894972317535102\t1970-01-01T00:00:00.000Z\t38\t\n" +
                "-1486191244\t10\tfalse\tTEM\t0.858231145130\t0.7701\t541\t15346\t2015-09-07T03:59:42.626Z\t2015-03-29T07:45:14.234Z\tXSJG\t149\t4541604023029198395\t1970-01-01T00:16:40.000Z\t9\t\n" +
                "-1522891304\t9\ttrue\tYQH\t0.706531626801\t0.9593\t148\t1418\t2015-05-14T03:58:37.354Z\t2015-01-24T12:16:31.505Z\tVYKC\t150\t-8457611344460103622\t1970-01-01T00:33:20.000Z\t21\t\n" +
                "1714215360\tNaN\tfalse\tBJV\t0.737854058841\t0.8831\t961\t13649\t2015-03-26T07:59:51.760Z\t2015-01-08T13:03:59.451Z\t\t119\t-4671264890968938046\t1970-01-01T00:50:00.000Z\t27\t\n" +
                "538828647\tNaN\tfalse\tWCN\tNaN\t0.0484\t336\t458\t2015-09-30T16:45:09.581Z\t2015-01-28T00:22:40.576Z\tRFOJ\t178\t6729484365598176408\t1970-01-01T01:06:40.000Z\t35\t\n" +
                "1697518734\tNaN\tfalse\tTMN\t0.717182054828\t0.6008\t733\t28480\t2015-08-12T06:33:25.576Z\t2015-03-12T06:41:41.206Z\t\tNaN\t3736483738523783327\t1970-01-01T01:23:20.000Z\t45\t\n" +
                "-1400952716\t18\tfalse\t\t0.218894258011\t0.4857\t452\t-4513\t2015-08-19T11:42:22.030Z\t2015-05-03T02:51:04.411Z\tDFTX\t135\t-8304485221218681427\t1970-01-01T01:40:00.000Z\t23\t\n" +
                "375936447\tNaN\ttrue\tUBI\t0.368165199898\t0.2699\t365\t-22176\t2015-11-16T17:06:28.006Z\t2015-12-31T03:43:35.338Z\tVYKC\t111\t8010739590549067645\t1970-01-01T01:56:40.000Z\t40\t\n" +
                "129116128\t26\ttrue\tYZL\t0.813064425686\t0.8338\t858\t-10081\t2015-05-03T12:38:00.953Z\t2015-11-25T09:31:16.961Z\t\tNaN\t5948756226674494760\t1970-01-01T02:13:20.000Z\t31\t\n" +
                "-432463104\t14\ttrue\tRFU\t0.175424280181\tNaN\t664\t-1801\t2015-03-26T06:43:07.945Z\t2015-10-23T00:10:08.937Z\t\t133\t-6468262107294695357\t1970-01-01T02:30:00.000Z\t10\t\n" +
                "-183397337\t21\tfalse\t\t0.012473622922\t0.9454\t479\t13799\t2015-12-10T07:34:32.318Z\t2015-01-15T08:02:08.981Z\tVYKC\t111\t-4465272122610111023\t1970-01-01T02:46:40.000Z\t48\t\n" +
                "867442903\t12\tfalse\t\t0.130857524189\t0.4451\t742\t-5335\t2015-07-18T05:38:52.864Z\t\t\t182\t-7450331737317429481\t1970-01-01T03:03:20.000Z\t50\t\n" +
                "-556147011\t15\tfalse\tTFQ\t0.438312150997\t0.2030\t528\t-9881\t\t2015-03-14T07:02:27.211Z\tDFTX\tNaN\t8106546376718214472\t1970-01-01T03:20:00.000Z\t16\t\n" +
                "1152615007\t8\ttrue\tGZC\tNaN\t0.4102\t1009\t30832\t2015-07-06T20:09:56.948Z\t\t\t169\t8493737977074142374\t1970-01-01T03:36:40.000Z\t46\t\n" +
                "1556385852\t5\ttrue\tITE\t0.166455496716\t0.1111\t938\t-17264\t2015-03-18T23:15:07.551Z\t2015-08-23T20:56:49.842Z\tRFOJ\tNaN\t1960617583584840212\t1970-01-01T03:53:20.000Z\t50\t\n" +
                "-1579236077\tNaN\tfalse\tMGP\t0.721746066403\t0.0976\t153\t26419\t2015-11-04T22:06:21.848Z\t2015-07-25T21:06:19.201Z\tRFOJ\t155\t4783430750716839799\t1970-01-01T04:10:00.000Z\t17\t\n" +
                "-15359630\t28\ttrue\tXUZ\t0.824360876525\t0.6308\t731\t31115\t2015-04-20T08:35:41.892Z\t2015-12-23T06:38:09.765Z\tVYKC\tNaN\t-8202095271368279379\t1970-01-01T04:26:40.000Z\t12\t\n" +
                "747806015\tNaN\ttrue\tLBY\t0.100419896917\t0.5437\t517\t11082\t2015-09-20T18:12:24.863Z\t2015-05-29T16:46:02.629Z\t\t160\t-5210506690456942311\t1970-01-01T04:43:20.000Z\t46\t\n" +
                "-593635423\t2\ttrue\tYMC\t0.098259145850\t0.3786\t689\t1044\t2015-12-26T16:55:37.233Z\t\t\t159\t-637774648064250845\t1970-01-01T05:00:00.000Z\t20\t\n" +
                "1693060654\t24\ttrue\tCUO\t0.125607606275\t0.2555\t936\t24595\t2015-08-13T12:25:41.880Z\t2015-10-19T11:11:56.229Z\tVYKC\t159\t-2824299791517032037\t1970-01-01T05:16:40.000Z\t12\t\n";

        String expectedMeta = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"a1\",\"type\":\"INT\"},{\"index\":1,\"name\":\"a\",\"type\":\"INT\"},{\"index\":2,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":3,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":4,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":5,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":6,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":7,\"name\":\"f1\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"h\",\"type\":\"TIMESTAMP\"},{\"index\":10,\"name\":\"i\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"j1\",\"type\":\"LONG\"},{\"index\":13,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":14,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":15,\"name\":\"m\",\"type\":\"BINARY\"}],\"timestampIndex\":13}";

        assertCast(expectedData, expectedMeta, "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 123," + // first 64 bits of random seed
                " 345," + // second 64 bits of random seed
                " 'a1', rnd_int()," +
                " 'a', rnd_int(0, 30, 2)," +
                " 'b', rnd_boolean()," +
                " 'c', rnd_str(3,3,2)," +
                " 'd', rnd_double(2)," +
                " 'e', rnd_float(2)," +
                " 'f', rnd_short(10,1024)," +
                " 'f1', rnd_short()," +
                " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                " 'i', rnd_symbol(4,4,4,2)," +
                " 'j', rnd_long(100,200,2)," +
                " 'j1', rnd_long()," +
                " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                " 'l', rnd_byte(2,50)," +
                " 'm', rnd_bin(10, 20, 2)" +
                "))  timestamp(k) partition by DAY");
    }

    private void assertCast(String expectedData, String expectedMeta, String sql) throws SqlException, IOException {
        compiler.execute(sql, bindVariableService);
        try (TableReader reader = engine.getReader("y")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(expectedMeta, sink);

            sink.clear();
            printer.print(reader.getCursor(), true);
            TestUtils.assertEquals(expectedData, sink);
        }
    }

    @Test
    public void testDuplicateTableName() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try {
            compiler.execute("create table x (" +
                            "t TIMESTAMP, " +
                            "y BOOLEAN) " +
                            "timestamp(t) " +
                            "partition by MONTH",
                    bindVariableService
            );
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(13, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "table already exists");
        }
    }
}