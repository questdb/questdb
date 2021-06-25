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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HaversineDistDegreeGroupByFunctionFactoryTest extends AbstractGriffinTest {

    public static final double DELTA = 0.0001;

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void test10Rows() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = -5;
            double lonDegree = -6;
            long ts = 0;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1414.545985354098, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void test10RowsAndNullAtEnd() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = -5;
            double lonDegree = -6;
            long ts = 0;
            TableWriter.Row r;
            for (int i = 0; i < 10; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1414.545985354098, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void test2DistancesAtEquator() throws SqlException {

        compiler.compile("create table tab1 (lat double, lon double, k timestamp)", sqlExecutionContext);
        compiler.compile("create table tab2 (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab1", "testing")) {
            double lonDegree = 0;
            long ts = 0;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab2", "testing")) {
            double lonDegree = -180;
            long ts = 0;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }

        double distance1;
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab1", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                distance1 = record.getDouble(0);
            }
        }

        double distance2;
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab2", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                distance2 = record.getDouble(0);

            }
        }
        Assert.assertEquals(distance1, distance2, DELTA);
    }

    @Test
    public void test3Rows() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = 1;
            double lonDegree = 2;
            long ts = 0;
            for (int i = 0; i < 3; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(314.4073265716869, record.getDouble(0), DELTA);
            }
        }
    }

    //"select s, haversine_dist_deg(lat, lon, k) from tab",
    @Test
    public void testAggregationBySymbol() throws SqlException {

        compiler.compile("create table tab (s symbol, lat double, lon double, k timestamp) timestamp(k) partition by NONE", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = -5;
            double lonDegree = -6;
            long ts = 0;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putSym(0, "AAA");
                r.putDouble(1, latDegree);
                r.putDouble(2, lonDegree);
                r.putTimestamp(3, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 360000000L;
            }
            w.commit();
            latDegree = -20;
            lonDegree = 10;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putSym(0, "BBB");
                r.putDouble(1, latDegree);
                r.putDouble(2, lonDegree);
                r.putTimestamp(3, ts);
                r.append();
                latDegree += 0.1;
                lonDegree += 0.1;
                ts += 360000000L;
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select s, haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(2, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(1414.545985354098, record.getDouble(1), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("BBB", record.getSym(0));
                Assert.assertEquals(137.51028123371657, record.getDouble(1), DELTA);
            }
        }
    }

    @Test
    public void testAggregationBySymbolWithSampling() throws Exception {

        compiler.compile("create table tab (s symbol, lat double, lon double, p double,  k timestamp) timestamp(k) partition by NONE", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            long MICROS_IN_MIN = 60_000_000L;
            //row 1
            TableWriter.Row r = w.newRow(30 * MICROS_IN_MIN);
            r.putSym(0, "AAA");
            r.putDouble(1, -5);
            r.putDouble(2, 10);
            r.putDouble(3, 1000);
            r.append();
            //row 2
            r = w.newRow(90 * MICROS_IN_MIN);
            r.putSym(0, "AAA");
            r.putDouble(1, -4);
            r.putDouble(2, 11);
            r.putDouble(3, 1000);
            r.append();
            //row 3
            r = w.newRow(100 * MICROS_IN_MIN);
            r.putSym(0, "AAA");
            r.putDouble(1, -3);
            r.putDouble(2, 12);
            r.putDouble(3, 1000);
            r.append();
            //row 4
            r = w.newRow(210 * MICROS_IN_MIN);
            r.putSym(0, "AAA");
            r.putDouble(1, -2);
            r.putDouble(2, 13);
            r.putDouble(3, 1000);
            r.append();
            //row 5
            r = w.newRow(270 * MICROS_IN_MIN);
            r.putSym(0, "AAA");
            r.putDouble(1, -1);
            r.putDouble(2, 14);
            r.putDouble(3, 1000);
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select s, haversine_dist_deg(lat, lon, k), sum(p) from tab sample by 1h fill(linear)", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(5, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(78.50616567866791, record.getDouble(1), DELTA);
                Assert.assertEquals(1000, record.getDouble(2), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(264.19224423853797, record.getDouble(1), DELTA);
                Assert.assertEquals(2000, record.getDouble(2), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(85.73439427824682, record.getDouble(1), DELTA);
                Assert.assertEquals(1500, record.getDouble(2), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(121.48099900324064, record.getDouble(1), DELTA);
                Assert.assertEquals(1000, record.getDouble(2), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(78.61380186411724, record.getDouble(1), DELTA);
                Assert.assertEquals(1000, record.getDouble(2), DELTA);
            }
        }
    }

    //select s, haversine_dist_deg(lat, lon, k), k from tab sample by 3h fill(linear)
    @Test
    public void testAggregationWithSampleFill1() throws Exception {

        assertQuery("s\tlat\tlon\tk\n" +
                        "VTJW\t-5.0\t-6.0\t1970-01-03T00:31:40.000000Z\n" +
                        "VTJW\t-4.0\t-5.0\t1970-01-03T01:03:20.000000Z\n" +
                        "VTJW\t-3.0\t-4.0\t1970-01-03T01:35:00.000000Z\n" +
                        "VTJW\t-2.0\t-3.0\t1970-01-03T02:06:40.000000Z\n" +
                        "VTJW\t-1.0\t-2.0\t1970-01-03T02:38:20.000000Z\n" +
                        "VTJW\t0.0\t-1.0\t1970-01-03T03:10:00.000000Z\n",
                "tab",
                "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol(1,4,4,0) s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(174700000000, 1900000000) k" +
                        " from" +
                        " long_sequence(6)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into tab select * from (" +
                        "select" +
                        " rnd_symbol(2,4,4,0) s," +
                        " (-40.0 + (1*x)) lat," +
                        " (5.0 + (1*x)) lon," +
                        " timestamp_sequence(227200000000, 1900000000) k" +
                        " from" +
                        " long_sequence(3)" +
                        ") timestamp(k)",
                "s\tlat\tlon\tk\n" +
                        "VTJW\t-5.0\t-6.0\t1970-01-03T00:31:40.000000Z\n" +
                        "VTJW\t-4.0\t-5.0\t1970-01-03T01:03:20.000000Z\n" +
                        "VTJW\t-3.0\t-4.0\t1970-01-03T01:35:00.000000Z\n" +
                        "VTJW\t-2.0\t-3.0\t1970-01-03T02:06:40.000000Z\n" +
                        "VTJW\t-1.0\t-2.0\t1970-01-03T02:38:20.000000Z\n" +
                        "VTJW\t0.0\t-1.0\t1970-01-03T03:10:00.000000Z\n" +
                        "RXGZ\t-39.0\t6.0\t1970-01-03T15:06:40.000000Z\n" +
                        "RXGZ\t-38.0\t7.0\t1970-01-03T15:38:20.000000Z\n" +
                        "RXGZ\t-37.0\t8.0\t1970-01-03T16:10:00.000000Z\n",
                true, true, true);

        assertQuery("s\thaversine_dist_deg\tk\n" +
                        "VTJW\t140.48471753024785\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t297.7248158372856\t1970-01-03T01:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T01:00:00.000000Z\n" +
                        "VTJW\t297.911239737792\t1970-01-03T02:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T02:00:00.000000Z\n" +
                        "VTJW\t49.65838525039151\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T04:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T04:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T05:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T05:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T07:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T07:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T08:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T08:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T10:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T10:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T11:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T11:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T13:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T13:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T14:00:00.000000Z\n" +
                        "RXGZ\t268.93561321686246\t1970-01-03T14:00:00.000000Z\n" +
                        "RXGZ\t141.19868683690248\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t0.0\t1970-01-03T16:00:00.000000Z\n" +
                        "VTJW\t297.950311502349\t1970-01-03T16:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon, k), k from tab sample by 1h fill(linear)",
                null,
                "k",
                true, true, true);

    }

    @Test
    public void testAggregationWithSampleFill2_DataStartsOnTheClock() throws Exception {
        assertQuery("s\tlat\tlon\tk\n" +
                        "AAA\t-5.0\t-6.0\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t-4.0\t-5.0\t1970-01-01T00:10:00.000000Z\n" +
                        "AAA\t-3.0\t-4.0\t1970-01-01T00:20:00.000000Z\n" +
                        "AAA\t-2.0\t-3.0\t1970-01-01T00:30:00.000000Z\n" +
                        "AAA\t-1.0\t-2.0\t1970-01-01T00:40:00.000000Z\n" +
                        "AAA\t0.0\t-1.0\t1970-01-01T00:50:00.000000Z\n" +
                        "AAA\t1.0\t0.0\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t2.0\t1.0\t1970-01-01T01:10:00.000000Z\n" +
                        "AAA\t3.0\t2.0\t1970-01-01T01:20:00.000000Z\n" +
                        "AAA\t4.0\t3.0\t1970-01-01T01:30:00.000000Z\n" +
                        "AAA\t5.0\t4.0\t1970-01-01T01:40:00.000000Z\n" +
                        "AAA\t6.0\t5.0\t1970-01-01T01:50:00.000000Z\n" +
                        "AAA\t7.0\t6.0\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t8.0\t7.0\t1970-01-01T02:10:00.000000Z\n" +
                        "AAA\t9.0\t8.0\t1970-01-01T02:20:00.000000Z\n" +
                        "AAA\t10.0\t9.0\t1970-01-01T02:30:00.000000Z\n" +
                        "AAA\t11.0\t10.0\t1970-01-01T02:40:00.000000Z\n" +
                        "AAA\t12.0\t11.0\t1970-01-01T02:50:00.000000Z\n" +
                        "AAA\t13.0\t12.0\t1970-01-01T03:00:00.000000Z\n" +
                        "AAA\t14.0\t13.0\t1970-01-01T03:10:00.000000Z\n"
                , "tab", "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AAA') s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(0, 600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE", "k", true, false, true);

        assertQuery("s\thaversine_dist_deg\tk\n" +
                        "AAA\t943.0307116486234\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t942.1704436827788\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t936.1854124136329\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t155.09709548701773\t1970-01-01T03:00:00.000000Z\n"
                , "select s, haversine_dist_deg(lat, lon, k), k from tab sample by 1h fill(linear)", null, "k", true, true, true);
    }

    @Test
    public void testAggregationWithSampleFill3() throws Exception {

        assertQuery("s	lat	lon	k\n" +
                        "AAA\t-5.0\t-6.0\t1970-01-01T00:00:01.000000Z\n" +
                        "AAA\t-4.0\t-5.0\t1970-01-01T00:08:21.000000Z\n" +
                        "AAA\t-3.0\t-4.0\t1970-01-01T00:16:41.000000Z\n" +
                        "AAA\t-2.0\t-3.0\t1970-01-01T00:25:01.000000Z\n" +
                        "AAA\t-1.0\t-2.0\t1970-01-01T00:33:21.000000Z\n" +
                        "AAA\t0.0\t-1.0\t1970-01-01T00:41:41.000000Z\n" +
                        "AAA\t1.0\t0.0\t1970-01-01T00:50:01.000000Z\n" +
                        "AAA\t2.0\t1.0\t1970-01-01T00:58:21.000000Z\n" +
                        "AAA\t3.0\t2.0\t1970-01-01T01:06:41.000000Z\n" +
                        "AAA\t4.0\t3.0\t1970-01-01T01:15:01.000000Z\n" +
                        "AAA\t5.0\t4.0\t1970-01-01T01:23:21.000000Z\n" +
                        "AAA\t6.0\t5.0\t1970-01-01T01:31:41.000000Z\n" +
                        "AAA\t7.0\t6.0\t1970-01-01T01:40:01.000000Z\n" +
                        "AAA\t8.0\t7.0\t1970-01-01T01:48:21.000000Z\n" +
                        "AAA\t9.0\t8.0\t1970-01-01T01:56:41.000000Z\n" +
                        "AAA\t10.0\t9.0\t1970-01-01T02:05:01.000000Z\n" +
                        "AAA\t11.0\t10.0\t1970-01-01T02:13:21.000000Z\n" +
                        "AAA\t12.0\t11.0\t1970-01-01T02:21:41.000000Z\n" +
                        "AAA\t13.0\t12.0\t1970-01-01T02:30:01.000000Z\n" +
                        "AAA\t14.0\t13.0\t1970-01-01T02:38:21.000000Z\n",
                "tab",
                "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AAA') s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(1000000, 500000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                true, true, true);

        assertQuery("s\thaversine_dist_deg\tk\n" +
                        "AAA\t1131.3799004998614\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t1128.9573035397307\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t716.1464591924607\t1970-01-01T02:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon, k), k from tab sample by 1h fill(linear)",
                null,
                "k",
                true, true, true);
    }

    @Test
    public void testAggregationWithSampleFill4() throws Exception {

        assertQuery("s\tlat\tlon\tk\n" +
                        "AAA\t-5.0\t-6.0\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t-4.0\t-5.0\t1970-01-01T00:30:00.000000Z\n" +
                        "AAA\t-3.0\t-4.0\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t-2.0\t-3.0\t1970-01-01T01:30:00.000000Z\n" +
                        "AAA\t-1.0\t-2.0\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t0.0\t-1.0\t1970-01-01T02:30:00.000000Z\n" +
                        "AAA\t1.0\t0.0\t1970-01-01T03:00:00.000000Z\n" +
                        "AAA\t2.0\t1.0\t1970-01-01T03:30:00.000000Z\n",
                "tab",
                "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AAA') s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(0, 1800000000) k" +
                        " from" +
                        " long_sequence(8)" +
                        ") timestamp(k) partition by NONE",
                "k",
                true, true, true);

        assertQuery("s\thaversine_dist_deg\tk\n" +
                        "AAA\t314.1202784911236\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t314.4073265716869\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t314.5031065858129\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t157.22760372823444\t1970-01-01T03:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon, k), k from tab sample by 1h fill(linear)",
                null,
                "k",
                true, true, true);
    }

    // TODO Fix, see branch fix-haversine-test-attempt and run
    // mvn test -Dtest=O3FailureTest,O3CommitLagTest,HaversineDistDegreeGroupByFunctionFactoryTest
    // for stable reproduce
    //select s, haversine_dist_deg(lat, lon, k), k from tab sample by 3h fill(linear)
    @Test
    @Ignore
    public void testAggregationWithSampleFill5() throws Exception {
        class ParseHelper {
            public Record[] parse(String raw) throws NumericException {
                String[] rows = raw.split("\\n");
                Record[] records = new Record[rows.length];
                for (int i = 0; i < rows.length; ++i) {
                    String row = rows[i];
                    String[] cols = row.split("\\t");
                    records[i] = cols.length == 4 ? parse4(cols) : parse3(cols);
                }
                return records;
            }

            Record parse4(String[] cols) throws NumericException {
                Assert.assertEquals(cols.length, 4);
                final CharSequence s = cols[0];
                final double lat = Double.parseDouble(cols[1]);
                final double lon = Double.parseDouble(cols[2]);
                final long k = TimestampFormatUtils.parseTimestamp(cols[3]);
                return new Record() {
                    @Override
                    public CharSequence getSym(int col) {
                        return s.length()>0 ? s : null;
                    }

                    @Override
                    public double getDouble(int col) {
                        return col == 1 ? lat : lon;
                    }

                    @Override
                    public long getTimestamp(int col) {
                        return k;
                    }
                };
            }

            Record parse3(String[] cols) throws NumericException {
                Assert.assertEquals(cols.length, 3);
                final CharSequence s = cols[0];
                final double h = Double.parseDouble(cols[1]);
                final long k = TimestampFormatUtils.parseTimestamp(cols[2]);
                return new Record() {
                    @Override
                    public CharSequence getSym(int col) {
                        return s.length()>0 ? s : null;
                    }

                    @Override
                    public double getDouble(int col) {
                        return h;
                    }

                    @Override
                    public long getTimestamp(int col) {
                        return k;
                    }
                };
            }
        }

        String raw = "\t-5.0\t-6.0\t1970-01-03T00:00:00.000000Z\n" +
                "\t-4.0\t-5.0\t1970-01-03T00:06:00.000000Z\n" +
                "HYRX\t-3.0\t-4.0\t1970-01-03T00:12:00.000000Z\n" +
                "\t-2.0\t-3.0\t1970-01-03T00:18:00.000000Z\n" +
                "VTJW\t-1.0\t-2.0\t1970-01-03T00:24:00.000000Z\n" +
                "VTJW\t0.0\t-1.0\t1970-01-03T00:30:00.000000Z\n" +
                "VTJW\t1.0\t0.0\t1970-01-03T00:36:00.000000Z\n" +
                "\t2.0\t1.0\t1970-01-03T00:42:00.000000Z\n" +
                "RXGZ\t3.0\t2.0\t1970-01-03T00:48:00.000000Z\n" +
                "RXGZ\t4.0\t3.0\t1970-01-03T00:54:00.000000Z\n" +
                "\t5.0\t4.0\t1970-01-03T01:00:00.000000Z\n" +
                "PEHN\t6.0\t5.0\t1970-01-03T01:06:00.000000Z\n" +
                "VTJW\t7.0\t6.0\t1970-01-03T01:12:00.000000Z\n" +
                "\t8.0\t7.0\t1970-01-03T01:18:00.000000Z\n" +
                "\t9.0\t8.0\t1970-01-03T01:24:00.000000Z\n" +
                "CPSW\t10.0\t9.0\t1970-01-03T01:30:00.000000Z\n" +
                "PEHN\t11.0\t10.0\t1970-01-03T01:36:00.000000Z\n" +
                "VTJW\t12.0\t11.0\t1970-01-03T01:42:00.000000Z\n" +
                "HYRX\t13.0\t12.0\t1970-01-03T01:48:00.000000Z\n" +
                "\t14.0\t13.0\t1970-01-03T01:54:00.000000Z\n" +
                "\t15.0\t14.0\t1970-01-03T02:00:00.000000Z\n" +
                "VTJW\t16.0\t15.0\t1970-01-03T02:06:00.000000Z\n" +
                "PEHN\t17.0\t16.0\t1970-01-03T02:12:00.000000Z\n" +
                "\t18.0\t17.0\t1970-01-03T02:18:00.000000Z\n" +
                "PEHN\t19.0\t18.0\t1970-01-03T02:24:00.000000Z\n" +
                "\t20.0\t19.0\t1970-01-03T02:30:00.000000Z\n" +
                "CPSW\t21.0\t20.0\t1970-01-03T02:36:00.000000Z\n" +
                "PEHN\t22.0\t21.0\t1970-01-03T02:42:00.000000Z\n" +
                "CPSW\t23.0\t22.0\t1970-01-03T02:48:00.000000Z\n" +
                "VTJW\t24.0\t23.0\t1970-01-03T02:54:00.000000Z\n" +
                "VTJW\t25.0\t24.0\t1970-01-03T03:00:00.000000Z\n" +
                "\t26.0\t25.0\t1970-01-03T03:06:00.000000Z\n" +
                "PEHN\t27.0\t26.0\t1970-01-03T03:12:00.000000Z\n" +
                "\t28.0\t27.0\t1970-01-03T03:18:00.000000Z\n" +
                "\t29.0\t28.0\t1970-01-03T03:24:00.000000Z\n" +
                "\t30.0\t29.0\t1970-01-03T03:30:00.000000Z\n" +
                "\t31.0\t30.0\t1970-01-03T03:36:00.000000Z\n" +
                "\t32.0\t31.0\t1970-01-03T03:42:00.000000Z\n" +
                "\t33.0\t32.0\t1970-01-03T03:48:00.000000Z\n" +
                "\t34.0\t33.0\t1970-01-03T03:54:00.000000Z\n" +
                "\t35.0\t34.0\t1970-01-03T04:00:00.000000Z\n" +
                "PEHN\t36.0\t35.0\t1970-01-03T04:06:00.000000Z\n" +
                "RXGZ\t37.0\t36.0\t1970-01-03T04:12:00.000000Z\n" +
                "\t38.0\t37.0\t1970-01-03T04:18:00.000000Z\n" +
                "\t39.0\t38.0\t1970-01-03T04:24:00.000000Z\n" +
                "\t40.0\t39.0\t1970-01-03T04:30:00.000000Z\n" +
                "CPSW\t41.0\t40.0\t1970-01-03T04:36:00.000000Z\n" +
                "PEHN\t42.0\t41.0\t1970-01-03T04:42:00.000000Z\n" +
                "RXGZ\t43.0\t42.0\t1970-01-03T04:48:00.000000Z\n" +
                "VTJW\t44.0\t43.0\t1970-01-03T04:54:00.000000Z\n" +
                "RXGZ\t45.0\t44.0\t1970-01-03T05:00:00.000000Z\n" +
                "\t46.0\t45.0\t1970-01-03T05:06:00.000000Z\n" +
                "\t47.0\t46.0\t1970-01-03T05:12:00.000000Z\n" +
                "HYRX\t48.0\t47.0\t1970-01-03T05:18:00.000000Z\n" +
                "\t49.0\t48.0\t1970-01-03T05:24:00.000000Z\n" +
                "\t50.0\t49.0\t1970-01-03T05:30:00.000000Z\n" +
                "RXGZ\t51.0\t50.0\t1970-01-03T05:36:00.000000Z\n" +
                "RXGZ\t52.0\t51.0\t1970-01-03T05:42:00.000000Z\n" +
                "CPSW\t53.0\t52.0\t1970-01-03T05:48:00.000000Z\n" +
                "\t54.0\t53.0\t1970-01-03T05:54:00.000000Z\n" +
                "RXGZ\t55.0\t54.0\t1970-01-03T06:00:00.000000Z\n" +
                "CPSW\t56.0\t55.0\t1970-01-03T06:06:00.000000Z\n" +
                "\t57.0\t56.0\t1970-01-03T06:12:00.000000Z\n" +
                "\t58.0\t57.0\t1970-01-03T06:18:00.000000Z\n" +
                "\t59.0\t58.0\t1970-01-03T06:24:00.000000Z\n" +
                "HYRX\t60.0\t59.0\t1970-01-03T06:30:00.000000Z\n" +
                "PEHN\t61.0\t60.0\t1970-01-03T06:36:00.000000Z\n" +
                "\t62.0\t61.0\t1970-01-03T06:42:00.000000Z\n" +
                "\t63.0\t62.0\t1970-01-03T06:48:00.000000Z\n" +
                "PEHN\t64.0\t63.0\t1970-01-03T06:54:00.000000Z\n" +
                "\t65.0\t64.0\t1970-01-03T07:00:00.000000Z\n" +
                "\t66.0\t65.0\t1970-01-03T07:06:00.000000Z\n" +
                "VTJW\t67.0\t66.0\t1970-01-03T07:12:00.000000Z\n" +
                "PEHN\t68.0\t67.0\t1970-01-03T07:18:00.000000Z\n" +
                "\t69.0\t68.0\t1970-01-03T07:24:00.000000Z\n" +
                "\t70.0\t69.0\t1970-01-03T07:30:00.000000Z\n" +
                "CPSW\t71.0\t70.0\t1970-01-03T07:36:00.000000Z\n" +
                "RXGZ\t72.0\t71.0\t1970-01-03T07:42:00.000000Z\n" +
                "\t73.0\t72.0\t1970-01-03T07:48:00.000000Z\n" +
                "HYRX\t74.0\t73.0\t1970-01-03T07:54:00.000000Z\n" +
                "CPSW\t75.0\t74.0\t1970-01-03T08:00:00.000000Z\n" +
                "\t76.0\t75.0\t1970-01-03T08:06:00.000000Z\n" +
                "\t77.0\t76.0\t1970-01-03T08:12:00.000000Z\n" +
                "\t78.0\t77.0\t1970-01-03T08:18:00.000000Z\n" +
                "VTJW\t79.0\t78.0\t1970-01-03T08:24:00.000000Z\n" +
                "CPSW\t80.0\t79.0\t1970-01-03T08:30:00.000000Z\n" +
                "VTJW\t81.0\t80.0\t1970-01-03T08:36:00.000000Z\n" +
                "\t82.0\t81.0\t1970-01-03T08:42:00.000000Z\n" +
                "\t83.0\t82.0\t1970-01-03T08:48:00.000000Z\n" +
                "\t84.0\t83.0\t1970-01-03T08:54:00.000000Z\n" +
                "VTJW\t85.0\t84.0\t1970-01-03T09:00:00.000000Z\n" +
                "\t86.0\t85.0\t1970-01-03T09:06:00.000000Z\n" +
                "\t87.0\t86.0\t1970-01-03T09:12:00.000000Z\n" +
                "\t88.0\t87.0\t1970-01-03T09:18:00.000000Z\n" +
                "PEHN\t89.0\t88.0\t1970-01-03T09:24:00.000000Z\n" +
                "HYRX\t90.0\t89.0\t1970-01-03T09:30:00.000000Z\n" +
                "\t91.0\t90.0\t1970-01-03T09:36:00.000000Z\n" +
                "\t92.0\t91.0\t1970-01-03T09:42:00.000000Z\n" +
                "CPSW\t93.0\t92.0\t1970-01-03T09:48:00.000000Z\n" +
                "\t94.0\t93.0\t1970-01-03T09:54:00.000000Z\n";

        String raw2 = "\t-39.0\t6.0\t1970-01-04T05:00:00.000000Z\n" +
                "SUQS\t-38.0\t7.0\t1970-01-04T05:06:00.000000Z\n" +
                "OJIP\t-37.0\t8.0\t1970-01-04T05:12:00.000000Z\n" +
                "SUQS\t-36.0\t9.0\t1970-01-04T05:18:00.000000Z\n" +
                "\t-35.0\t10.0\t1970-01-04T05:24:00.000000Z\n" +
                "\t-34.0\t11.0\t1970-01-04T05:30:00.000000Z\n" +
                "RLTK\t-33.0\t12.0\t1970-01-04T05:36:00.000000Z\n" +
                "\t-32.0\t13.0\t1970-01-04T05:42:00.000000Z\n" +
                "SUQS\t-31.0\t14.0\t1970-01-04T05:48:00.000000Z\n" +
                "RLTK\t-30.0\t15.0\t1970-01-04T05:54:00.000000Z\n" +
                "SUQS\t-29.0\t16.0\t1970-01-04T06:00:00.000000Z\n" +
                "OJIP\t-28.0\t17.0\t1970-01-04T06:06:00.000000Z\n" +
                "\t-27.0\t18.0\t1970-01-04T06:12:00.000000Z\n" +
                "\t-26.0\t19.0\t1970-01-04T06:18:00.000000Z\n" +
                "\t-25.0\t20.0\t1970-01-04T06:24:00.000000Z\n" +
                "VVSJ\t-24.0\t21.0\t1970-01-04T06:30:00.000000Z\n" +
                "HZEP\t-23.0\t22.0\t1970-01-04T06:36:00.000000Z\n" +
                "RLTK\t-22.0\t23.0\t1970-01-04T06:42:00.000000Z\n" +
                "\t-21.0\t24.0\t1970-01-04T06:48:00.000000Z\n" +
                "\t-20.0\t25.0\t1970-01-04T06:54:00.000000Z\n" +
                "\t-19.0\t26.0\t1970-01-04T07:00:00.000000Z\n" +
                "\t-18.0\t27.0\t1970-01-04T07:06:00.000000Z\n" +
                "HZEP\t-17.0\t28.0\t1970-01-04T07:12:00.000000Z\n" +
                "\t-16.0\t29.0\t1970-01-04T07:18:00.000000Z\n" +
                "\t-15.0\t30.0\t1970-01-04T07:24:00.000000Z\n" +
                "HZEP\t-14.0\t31.0\t1970-01-04T07:30:00.000000Z\n" +
                "\t-13.0\t32.0\t1970-01-04T07:36:00.000000Z\n" +
                "RLTK\t-12.0\t33.0\t1970-01-04T07:42:00.000000Z\n" +
                "\t-11.0\t34.0\t1970-01-04T07:48:00.000000Z\n" +
                "HZEP\t-10.0\t35.0\t1970-01-04T07:54:00.000000Z\n" +
                "\t-9.0\t36.0\t1970-01-04T08:00:00.000000Z\n" +
                "\t-8.0\t37.0\t1970-01-04T08:06:00.000000Z\n" +
                "\t-7.0\t38.0\t1970-01-04T08:12:00.000000Z\n" +
                "\t-6.0\t39.0\t1970-01-04T08:18:00.000000Z\n" +
                "\t-5.0\t40.0\t1970-01-04T08:24:00.000000Z\n";

        ParseHelper helper = new ParseHelper();
        Record[] expected = helper.parse(raw);
        Record[] expected2 = helper.parse(raw + raw2);

        assertQuery(expected,
                "tab",
                "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into tab select * from (" +
                        "select" +
                        " rnd_symbol(5,4,4,1) b," +
                        " (-40.0 + (1*x)) lat," +
                        " (5.0 + (1*x)) lon," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                expected2,
                true,
                true
        );

        String haversineRaw = "\t1571.5578217325824\t1970-01-03T00:00:00.000000Z\n" +
                "HYRX\t1253.5510865162087\t1970-01-03T00:00:00.000000Z\n" +
                "VTJW\t942.6135685249942\t1970-01-03T00:00:00.000000Z\n" +
                "RXGZ\t307.90925537977535\t1970-01-03T00:00:00.000000Z\n" +
                "PEHN\t1548.9479780042082\t1970-01-03T00:00:00.000000Z\n" +
                "CPSW\t1542.7331966234847\t1970-01-03T00:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T00:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T00:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T00:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T00:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T00:00:00.000000Z\n" +
                "\t1559.6725170700527\t1970-01-03T01:00:00.000000Z\n" +
                "PEHN\t781.837711644479\t1970-01-03T01:00:00.000000Z\n" +
                "VTJW\t1559.5797769126589\t1970-01-03T01:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T01:00:00.000000Z\n" +
                "HYRX\t1542.4916842534376\t1970-01-03T01:00:00.000000Z\n" +
                "RXGZ\t1508.0130824598755\t1970-01-03T01:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T01:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T01:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T01:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T01:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T01:00:00.000000Z\n" +
                "\t1523.5770452169781\t1970-01-03T02:00:00.000000Z\n" +
                "VTJW\t1525.2538438445374\t1970-01-03T02:00:00.000000Z\n" +
                "PEHN\t763.935720895139\t1970-01-03T02:00:00.000000Z\n" +
                "CPSW\t303.26499352443494\t1970-01-03T02:00:00.000000Z\n" +
                "HYRX\t1444.702988686144\t1970-01-03T02:00:00.000000Z\n" +
                "RXGZ\t1508.0130824598755\t1970-01-03T02:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T02:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T02:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T02:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T02:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T02:00:00.000000Z\n" +
                "VTJW\t1433.537722110984\t1970-01-03T03:00:00.000000Z\n" +
                "\t1471.6172229773344\t1970-01-03T03:00:00.000000Z\n" +
                "PEHN\t0.0\t1970-01-03T03:00:00.000000Z\n" +
                "HYRX\t1444.702988686144\t1970-01-03T03:00:00.000000Z\n" +
                "RXGZ\t1508.0130824598755\t1970-01-03T03:00:00.000000Z\n" +
                "CPSW\t1451.7784008633935\t1970-01-03T03:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T03:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T03:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T03:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T03:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T03:00:00.000000Z\n" +
                "\t1397.9585136826277\t1970-01-03T04:00:00.000000Z\n" +
                "PEHN\t844.4750034893823\t1970-01-03T04:00:00.000000Z\n" +
                "RXGZ\t1415.4752733243026\t1970-01-03T04:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T04:00:00.000000Z\n" +
                "VTJW\t1416.283552629412\t1970-01-03T04:00:00.000000Z\n" +
                "HYRX\t1444.702988686144\t1970-01-03T04:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T04:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T04:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T04:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T04:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T04:00:00.000000Z\n" +
                "RXGZ\t1321.205074129582\t1970-01-03T05:00:00.000000Z\n" +
                "\t1322.7135481283856\t1970-01-03T05:00:00.000000Z\n" +
                "HYRX\t1333.169531838465\t1970-01-03T05:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T05:00:00.000000Z\n" +
                "VTJW\t1260.996027295267\t1970-01-03T05:00:00.000000Z\n" +
                "PEHN\t1299.0723115486594\t1970-01-03T05:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T05:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T05:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T05:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T05:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T05:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T06:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T06:00:00.000000Z\n" +
                "\t1244.2342427984713\t1970-01-03T06:00:00.000000Z\n" +
                "HYRX\t1235.94794305972\t1970-01-03T06:00:00.000000Z\n" +
                "PEHN\t367.34015194810576\t1970-01-03T06:00:00.000000Z\n" +
                "VTJW\t1260.996027295267\t1970-01-03T06:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T06:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T06:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T06:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T06:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T06:00:00.000000Z\n" +
                "\t1175.2706393688966\t1970-01-03T07:00:00.000000Z\n" +
                "VTJW\t1174.4621159060787\t1970-01-03T07:00:00.000000Z\n" +
                "PEHN\t0.0\t1970-01-03T07:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T07:00:00.000000Z\n" +
                "RXGZ\t845.0992051963561\t1970-01-03T07:00:00.000000Z\n" +
                "HYRX\t1179.0702290284257\t1970-01-03T07:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T07:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T07:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T07:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T07:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T07:00:00.000000Z\n" +
                "CPSW\t568.3469965005941\t1970-01-03T08:00:00.000000Z\n" +
                "\t1129.8289167844353\t1970-01-03T08:00:00.000000Z\n" +
                "VTJW\t1134.62839826717\t1970-01-03T08:00:00.000000Z\n" +
                "HYRX\t1111.9646253430044\t1970-01-03T08:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T08:00:00.000000Z\n" +
                "PEHN\t1115.6349817660346\t1970-01-03T08:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T08:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T08:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T08:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T08:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T08:00:00.000000Z\n" +
                "VTJW\t0.0\t1970-01-03T09:00:00.000000Z\n" +
                "\t1076.8372558202902\t1970-01-03T09:00:00.000000Z\n" +
                "PEHN\t0.0\t1970-01-03T09:00:00.000000Z\n" +
                "HYRX\t555.9823126715022\t1970-01-03T09:00:00.000000Z\n" +
                "CPSW\t0.0\t1970-01-03T09:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T09:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T09:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T09:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T09:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T09:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T09:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T10:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T10:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T10:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T10:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T10:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T10:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T10:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T10:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T10:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T10:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T10:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T11:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T11:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T11:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T11:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T11:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T11:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T11:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T11:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T11:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T11:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T11:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T12:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T12:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T12:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T12:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T12:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T12:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T12:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T12:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T12:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T12:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T12:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T13:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T13:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T13:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T13:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T13:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T13:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T13:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T13:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T13:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T13:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T13:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T14:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T14:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T14:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T14:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T14:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T14:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T14:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T14:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T14:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T14:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T14:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T15:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T15:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T15:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T15:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T15:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T15:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T15:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T15:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T15:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T15:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T15:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T16:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T16:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T16:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T16:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T16:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T16:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T16:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T16:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T16:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T16:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T16:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T17:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T17:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T17:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T17:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T17:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T17:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T17:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T17:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T17:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T17:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T17:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T18:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T18:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T18:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T18:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T18:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T18:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T18:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T18:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T18:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T18:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T18:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T19:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T19:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T19:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T19:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T19:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T19:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T19:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T19:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T19:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T19:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T19:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T20:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T20:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T20:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T20:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T20:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T20:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T20:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T20:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T20:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T20:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T20:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T21:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T21:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T21:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T21:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T21:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T21:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T21:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T21:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T21:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T21:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T21:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T22:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T22:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T22:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T22:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T22:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T22:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T22:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T22:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T22:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T22:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T22:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-03T23:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-03T23:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-03T23:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-03T23:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-03T23:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-03T23:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-03T23:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-03T23:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-03T23:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-03T23:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-03T23:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-04T00:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T00:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T00:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T00:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T00:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T00:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T00:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T00:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-04T00:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T00:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T00:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-04T01:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T01:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T01:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T01:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T01:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T01:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T01:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T01:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-04T01:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T01:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T01:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-04T02:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T02:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T02:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T02:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T02:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T02:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T02:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T02:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-04T02:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T02:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T02:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-04T03:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T03:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T03:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T03:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T03:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T03:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T03:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T03:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-04T03:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T03:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T03:00:00.000000Z\n" +
                "\t751.5729198940448\t1970-01-04T04:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T04:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T04:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T04:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T04:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T04:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T04:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T04:00:00.000000Z\n" +
                "RLTK\t1494.0627121088503\t1970-01-04T04:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T04:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T04:00:00.000000Z\n" +
                "\t1445.5242732671725\t1970-01-04T05:00:00.000000Z\n" +
                "SUQS\t1008.2717411536754\t1970-01-04T05:00:00.000000Z\n" +
                "OJIP\t0.0\t1970-01-04T05:00:00.000000Z\n" +
                "RLTK\t438.3365349251118\t1970-01-04T05:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T05:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T05:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T05:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T05:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T05:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T05:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T05:00:00.000000Z\n" +
                "SUQS\t0.0\t1970-01-04T06:00:00.000000Z\n" +
                "OJIP\t0.0\t1970-01-04T06:00:00.000000Z\n" +
                "\t1503.361660370032\t1970-01-04T06:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T06:00:00.000000Z\n" +
                "HZEP\t0.0\t1970-01-04T06:00:00.000000Z\n" +
                "RLTK\t0.0\t1970-01-04T06:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T06:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T06:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T06:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T06:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T06:00:00.000000Z\n" +
                "\t1548.4449468883656\t1970-01-04T07:00:00.000000Z\n" +
                "HZEP\t1085.3483977832977\t1970-01-04T07:00:00.000000Z\n" +
                "RLTK\t0.0\t1970-01-04T07:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T07:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T07:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T07:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T07:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T07:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T07:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T07:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T07:00:00.000000Z\n" +
                "\t626.6193073379977\t1970-01-04T08:00:00.000000Z\n" +
                "HYRX\t174.04663701020945\t1970-01-04T08:00:00.000000Z\n" +
                "VTJW\t1119.5224162334887\t1970-01-04T08:00:00.000000Z\n" +
                "RXGZ\t1207.2845788519373\t1970-01-04T08:00:00.000000Z\n" +
                "PEHN\t174.2822495520569\t1970-01-04T08:00:00.000000Z\n" +
                "CPSW\t1106.8786130678832\t1970-01-04T08:00:00.000000Z\n" +
                "SUQS\t1470.9189106882948\t1970-01-04T08:00:00.000000Z\n" +
                "OJIP\t1453.077628199122\t1970-01-04T08:00:00.000000Z\n" +
                "RLTK\t1537.305697064378\t1970-01-04T08:00:00.000000Z\n" +
                "VVSJ\t0.0\t1970-01-04T08:00:00.000000Z\n" +
                "HZEP\t1525.3785375874882\t1970-01-04T08:00:00.000000Z\n";
        Record[] haversineExp = helper.parse(haversineRaw);
        assertQuery(haversineExp,
                "select s, haversine_dist_deg(lat, lon, k), k from tab sample by 1h fill(linear)",
                null,
                "k",
                true, true);
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testCircumferenceAtEquator() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double lonDegree = -180;
            long ts = 0;
            for (int i = 0; i < 360; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(39919.53004981382, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testNegativeLatLon() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = -1;
            double lonDegree = -2;
            long ts = 0;
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree -= 1;
                lonDegree -= 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testOneNullAtEnd() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r;
            double latDegree = 1;
            double lonDegree = 2;
            long ts = 0;
            for (int i = 0; i < 2; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            r = w.newRow();
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testOneNullAtTop() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.append();
            double latDegree = 1;
            double lonDegree = 2;
            long ts = 0;
            for (int i = 0; i < 2; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testOneNullInMiddle() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.putDouble(0, 1);
            r.putDouble(1, 2);
            r.putTimestamp(2, 10_000_000_000L);
            r.append();
            r = w.newRow();
            r.append();
            r = w.newRow();
            r.putDouble(0, 2);
            r.putDouble(1, 3);
            r.putTimestamp(2, 20_000_000_000L);
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testOneNullsInMiddle() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            TableWriter.Row r = w.newRow();
            r.putDouble(0, 1);
            r.putDouble(1, 2);
            r.putTimestamp(2, 10_000_000_000L);
            r.append();
            r = w.newRow();
            r.append();
            r = w.newRow();
            r.append();
            r = w.newRow();
            r.append();
            r = w.newRow();
            r.putDouble(0, 2);
            r.putDouble(1, 3);
            r.putTimestamp(2, 20_000_000_000L);
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }

    @Test
    public void testPositiveLatLon() throws SqlException {

        compiler.compile("create table tab (lat double, lon double, k timestamp)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab", "testing")) {
            double latDegree = 1;
            double lonDegree = 2;
            long ts = 0;
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.putTimestamp(2, ts);
                r.append();
                latDegree += 1;
                lonDegree += 1;
                ts += 10_000_000_000L;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon, k) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }
}