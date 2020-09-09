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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab1", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                distance1 = record.getDouble(0);
            }
        }

        double distance2;
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab2", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(314.4073265716869, record.getDouble(0), DELTA);
            }
        }
    }

    //"select s, haversine_dist_degree(lat, lon) from tab",
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

        try (RecordCursorFactory factory = compiler.compile("select s, haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

    //select s, haversine_dist_degree(lat, lon), k from tab sample by 3h fill(linear)
    @Test
    public void testAggregationWithSampleFill() throws SqlException {

        compiler.compile("create table tab (s symbol, lat double, lon double, k timestamp) timestamp(k) partition by NONE", sqlExecutionContext);

        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tab")) {
            double aaaLatDegree = -5;
            double aaaLonDegree = -6;
            double bbbLatDegree = -20;
            double bbbLonDegree = 10;
            long ts = 0;
            for (int i = 0; i < 140; i++) {
                TableWriter.Row r = w.newRow();
                if (i % 2 == 0) {
                    r.putSym(0, "AAA");
                    r.putDouble(1, aaaLatDegree);
                    r.putDouble(2, aaaLonDegree);
                    aaaLatDegree += 1;
                    aaaLonDegree += 1;
                } else {
                    r.putSym(0, "BBB");
                    r.putDouble(1, bbbLatDegree);
                    r.putDouble(2, bbbLonDegree);
                    bbbLatDegree += 0.1;
                    bbbLonDegree += 0.1;
                }
                r.putTimestamp(3, ts);
                r.append();
                ts += 600_000_000L;
            }
            w.commit();
        }

        try (RecordCursorFactory factory = compiler.compile("select s, haversine_dist_degree(lat, lon), k from tab sample by 1h fill(linear)", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(24, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("AAA", record.getSym(0));
                Assert.assertEquals(9971.364670187557, record.getDouble(1), DELTA);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals("BBB", record.getSym(0));
                Assert.assertEquals(76.35009626485352, record.getDouble(1), DELTA);
            }
        }
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

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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

        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
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
        try (RecordCursorFactory factory = compiler.compile("select haversine_dist_degree(lat, lon) from tab", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                Assert.assertEquals(1, cursor.size());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(157.22760372823444, record.getDouble(0), DELTA);
            }
        }
    }
}