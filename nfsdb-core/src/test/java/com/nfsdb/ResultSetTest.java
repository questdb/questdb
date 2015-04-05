/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.model.TestEntity;
import com.nfsdb.query.ResultSet;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class ResultSetTest extends AbstractTest {

    @Test
    public void testReadColumns() throws Exception {

        JournalWriter<TestEntity> w = factory.writer(TestEntity.class);
        w.append(new TestEntity().setBStr("test1").setAnInt(10));
        w.append(new TestEntity().setDStr("test2").setADouble(55d));
        w.append(new TestEntity().setDwStr("test3").setSym("xyz"));
        w.commit();

        Journal<TestEntity> r = factory.reader(TestEntity.class);
        ResultSet<TestEntity> rs = r.query().all().asResultSet();
        Assert.assertEquals(3, rs.size());
        Assert.assertNull(rs.getSymbol(0, 4));
    }

    @Test
    public void testReadPrimitive() throws Exception {
        JournalWriter<TestEntity> w = factory.writer(TestEntity.class);
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
