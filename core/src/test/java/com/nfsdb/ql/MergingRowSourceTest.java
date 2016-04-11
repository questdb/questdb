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

package com.nfsdb.ql;

import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.impl.JournalPartitionSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.latest.HeapMergingRowSource;
import com.nfsdb.ql.impl.latest.KvIndexSymLookupRowSource;
import com.nfsdb.ql.impl.latest.MergingRowSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MergingRowSourceTest extends AbstractTest {
    @Test
    public void testHeapMerge() throws JournalException, NumericException {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, Dates.parseDateTime("2014-02-11T00:00:00.000Z"), 10);

        RowSource srcA = new KvIndexSymLookupRowSource("sym", "BP.L", true);
        RowSource srcB = new KvIndexSymLookupRowSource("sym", "WTB.L", true);

        RecordSource rs = new JournalSource(new JournalPartitionSource(w.getMetadata(), true), new HeapMergingRowSource(srcA, srcB));

        long last = 0;
        RecordCursor c = rs.prepareCursor(factory);
        int ts = rs.getMetadata().getColumnIndex("timestamp");
        while (c.hasNext()) {
            long r = c.next().getDate(ts);
            Assert.assertTrue(r > last);
            last = r;
        }
    }

    @Test
    public void testMerge() throws JournalException, NumericException {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, Dates.parseDateTime("2014-02-11T00:00:00.000Z"), 10);

        RowSource srcA = new KvIndexSymLookupRowSource("sym", "BP.L", true);
        RowSource srcB = new KvIndexSymLookupRowSource("sym", "WTB.L", true);

        RecordSource rs = new JournalSource(new JournalPartitionSource(w.getMetadata(), true), new MergingRowSource(srcA, srcB));

        long last = 0;
        RecordCursor c = rs.prepareCursor(factory);
        int ts = rs.getMetadata().getColumnIndex("timestamp");
        while (c.hasNext()) {
            long r = c.next().getDate(ts);
            Assert.assertTrue(r > last);
            last = r;
        }
    }

}
