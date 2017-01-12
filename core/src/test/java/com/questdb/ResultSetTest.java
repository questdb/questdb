/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb;

import com.questdb.misc.Dates;
import com.questdb.model.TestEntity;
import com.questdb.query.ResultSet;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ResultSetTest extends AbstractTest {

    @Test
    public void testReadColumns() throws Exception {

        try (JournalWriter<TestEntity> w = getWriterFactory().writer(TestEntity.class)) {
            w.append(new TestEntity().setBStr("test1").setAnInt(10));
            w.append(new TestEntity().setDStr("test2").setADouble(55d));
            w.append(new TestEntity().setDwStr("test3").setSym("xyz"));
            w.commit();
        }

        try (Journal<TestEntity> r = factoryContainer.getFactory().reader(TestEntity.class)) {
            ResultSet<TestEntity> rs = r.query().all().asResultSet();
            Assert.assertEquals(3, rs.size());
            Assert.assertNull(rs.getSymbol(0, 4));
        }
    }

    @Test
    public void testReadPrimitive() throws Exception {
        try (JournalWriter<TestEntity> w = getWriterFactory().writer(TestEntity.class)) {
            TestUtils.generateTestEntityData(w, 10000, Dates.parseDateTime("2012-05-15T10:55:00.000Z"), 100000);

            ResultSet<TestEntity> rs = w.query().all().asResultSet();

            int symIndex = w.getMetadata().getColumnIndex("sym");
            int doubleIndex = w.getMetadata().getColumnIndex("aDouble");
            int intIndex = w.getMetadata().getColumnIndex("anInt");
            int tsIndex = w.getMetadata().getColumnIndex("timestamp");
            int bStrIndex = w.getMetadata().getColumnIndex("bStr");
            int dStrIndex = w.getMetadata().getColumnIndex("dStr");
            int dwStrIndex = w.getMetadata().getColumnIndex("dwStr");

            for (int i = 0; i < rs.size(); i++) {
                TestEntity e = rs.read(i);
                Assert.assertEquals(e.getSym(), rs.getSymbol(i, symIndex));
                Assert.assertEquals(e.getADouble(), rs.getDouble(i, doubleIndex), 10);
                Assert.assertEquals(e.getAnInt(), rs.getInt(i, intIndex));
                Assert.assertEquals(e.getTimestamp(), rs.getLong(i, tsIndex));
                Assert.assertEquals(e.getBStr(), rs.getString(i, bStrIndex));
                Assert.assertEquals(e.getDStr(), rs.getString(i, dStrIndex));
                Assert.assertEquals(e.getDwStr(), rs.getString(i, dwStrIndex));
            }
        }
    }
}
