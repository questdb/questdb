/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
