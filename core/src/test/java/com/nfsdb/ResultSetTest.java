/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.misc.Dates;
import com.nfsdb.model.TestEntity;
import com.nfsdb.query.ResultSet;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
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
