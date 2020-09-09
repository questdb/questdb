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
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HaversineDistDegreeGroupByFunctionFactoryTest extends AbstractGriffinTest {

    public static final double DELTA = 0.0001;

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void test10Rows() throws SqlException {

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double latDegree = -5;
            double lonDegree = -6;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double latDegree = -5;
            double lonDegree = -6;
            TableWriter.Row r;
            for (int i = 0; i < 10; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
//            r = w.newRow();
//            r.append();
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab1 (lat double, lon double)", sqlExecutionContext);
        compiler.compile("create table tab2 (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab1")) {
            double lonDegree = 0;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.append();
                lonDegree += 1;
            }
            w.commit();
        }

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab2")) {
            double lonDegree = -180;
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.append();
                lonDegree += 1;
            }
            w.commit();
        }

        double distance1;
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab1", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                distance1 = record.getDouble(0);
            }
        }

        double distance2;
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab2", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double latDegree = 1;
            double lonDegree = 2;
            for (int i = 0; i < 3; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(314.4073265716869, record.getDouble(0), DELTA);
            }
        }
    }

    //"select s, haversine_dist_deg(lat, lon) from tab",
    @Test
    public void testAggregationBySymbol() throws SqlException {

        compiler.compile("create table tab (s symbol, lat double, lon double, k timestamp) timestamp(k) partition by NONE", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
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

        try (RecordCursorFactory factory = compiler.compile("select s, haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

    //select s, haversine_dist_deg(lat, lon), k from tab sample by 3h fill(linear)
    @Test
    public void testAggregationWithSampleFill1() throws Exception {

        assertQuery("s\tlat\tlon\tk\n" +
                        "AAA\t-5.0\t-6.0\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t-4.0\t-5.0\t1970-01-01T00:08:20.000000Z\n" +
                        "BBB\t-3.0\t-4.0\t1970-01-01T00:16:40.000000Z\n" +
                        "BBB\t-2.0\t-3.0\t1970-01-01T00:25:00.000000Z\n" +
                        "BBB\t-1.0\t-2.0\t1970-01-01T00:33:20.000000Z\n" +
                        "BBB\t0.0\t-1.0\t1970-01-01T00:41:40.000000Z\n" +
                        "AAA\t1.0\t0.0\t1970-01-01T00:50:00.000000Z\n" +
                        "BBB\t2.0\t1.0\t1970-01-01T00:58:20.000000Z\n" +
                        "AAA\t3.0\t2.0\t1970-01-01T01:06:40.000000Z\n" +
                        "AAA\t4.0\t3.0\t1970-01-01T01:15:00.000000Z\n" +
                        "AAA\t5.0\t4.0\t1970-01-01T01:23:20.000000Z\n" +
                        "AAA\t6.0\t5.0\t1970-01-01T01:31:40.000000Z\n" +
                        "AAA\t7.0\t6.0\t1970-01-01T01:40:00.000000Z\n" +
                        "BBB\t8.0\t7.0\t1970-01-01T01:48:20.000000Z\n" +
                        "BBB\t9.0\t8.0\t1970-01-01T01:56:40.000000Z\n" +
                        "AAA\t10.0\t9.0\t1970-01-01T02:05:00.000000Z\n" +
                        "AAA\t11.0\t10.0\t1970-01-01T02:13:20.000000Z\n" +
                        "BBB\t12.0\t11.0\t1970-01-01T02:21:40.000000Z\n" +
                        "BBB\t13.0\t12.0\t1970-01-01T02:30:00.000000Z\n" +
                        "AAA\t14.0\t13.0\t1970-01-01T02:38:20.000000Z\n",
                "tab",
                "create table tab as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AAA','BBB') s," +
                        " (-6.0 + (1*x)) lat," +
                        " (-7.0 + (1*x)) lon," +
                        " timestamp_sequence(0, 500000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                true, true, true);

        assertQuery("s\thaversine_dist_deg\tk\n" +
                        "AAA\t943.0302845043686\t1970-01-01T00:00:00.000000Z\n" +
                        "BBB\t786.1380286764727\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t627.7631171110919\t1970-01-01T01:00:00.000000Z\n" +
                        "BBB\t156.39320314017536\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t622.1211154227233\t1970-01-01T02:00:00.000000Z\n" +
                        "BBB\t155.40178053801114\t1970-01-01T02:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon), k from tab sample by 1h fill(linear)",
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
                        "AAA\t785.779158355717\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t785.4205536161624\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t780.7836318756217\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t155.09709548701773\t1970-01-01T03:00:00.000000Z\n"
                , "select s, haversine_dist_deg(lat, lon), k from tab sample by 1h fill(linear)", null, "k", true, true, true);
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
                        "AAA\t1100.2583153768578\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t940.7395858291213\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t622.1261899611127\t1970-01-01T02:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon), k from tab sample by 1h fill(linear)",
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
                        "AAA\t157.01233135733582\t1970-01-01T00:00:00.000000Z\n" +
                        "AAA\t157.17972284345245\t1970-01-01T01:00:00.000000Z\n" +
                        "AAA\t157.25155329290644\t1970-01-01T02:00:00.000000Z\n" +
                        "AAA\t157.22760372823444\t1970-01-01T03:00:00.000000Z\n",
                "select s, haversine_dist_deg(lat, lon), k from tab sample by 1h fill(linear)",
                null,
                "k",
                true, true, true);
    }

    @Test
    public void testAllNull() throws SqlException {

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.append();
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double lonDegree = -180;
            for (int i = 0; i < 360; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, 0);
                r.putDouble(1, lonDegree);
                r.append();
                lonDegree += 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double latDegree = -1;
            double lonDegree = -2;
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree -= 1;
                lonDegree -= 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r;
            double latDegree = 1;
            double lonDegree = 2;
            for (int i = 0; i < 2; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
            r = w.newRow();
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.append();
            double latDegree = 1;
            double lonDegree = 2;
            for (int i = 0; i < 2; i++) {
                r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.putDouble(0, 1);
            r.putDouble(1, 2);
            r.append();
            r = w.newRow();
            r.append();
            r = w.newRow();
            r.putDouble(0, 2);
            r.putDouble(1, 3);
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            TableWriter.Row r = w.newRow();
            r.putDouble(0, 1);
            r.putDouble(1, 2);
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
            r.append();
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        compiler.compile("create table tab (lat double, lon double)", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double latDegree = 1;
            double lonDegree = 2;
            for (int i = 0; i < 2; i++) {
                TableWriter.Row r = w.newRow();
                r.putDouble(0, latDegree);
                r.putDouble(1, lonDegree);
                r.append();
                latDegree += 1;
                lonDegree += 1;
            }
            w.commit();
        }
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_deg(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }
}