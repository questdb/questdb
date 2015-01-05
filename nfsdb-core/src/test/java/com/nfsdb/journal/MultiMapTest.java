/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal;

import com.nfsdb.journal.collections.mmap.MapValues;
import com.nfsdb.journal.collections.mmap.MultiMap;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.export.CharSink;
import com.nfsdb.journal.export.RecordSourcePrinter;
import com.nfsdb.journal.export.StringSink;
import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class MultiMapTest extends AbstractTest {

    private final CharSink sink = new StringSink();

    @Test
    public void testCount() throws Exception {

        final String expected = "186\t2014-12-30T03:08:00.000Z\tAGK.L\t\n" +
                "185\t2014-12-30T03:08:00.000Z\tBP.L\t\n" +
                "216\t2014-12-30T03:08:00.000Z\tRRS.L\t\n" +
                "196\t2014-12-30T03:08:00.000Z\tBT-A.L\t\n" +
                "214\t2014-12-30T03:08:00.000Z\tGKN.L\t\n" +
                "184\t2014-12-30T03:08:00.000Z\tLLOY.L\t\n" +
                "187\t2014-12-30T03:08:00.000Z\tABF.L\t\n" +
                "196\t2014-12-30T03:08:00.000Z\tWTB.L\t\n" +
                "193\t2014-12-30T03:08:00.000Z\tTLW.L\t\n" +
                "192\t2014-12-30T03:08:00.000Z\tADM.L\t\n" +
                "189\t2014-12-30T03:09:00.000Z\tBP.L\t\n" +
                "203\t2014-12-30T03:09:00.000Z\tGKN.L\t\n" +
                "201\t2014-12-30T03:09:00.000Z\tADM.L\t\n" +
                "187\t2014-12-30T03:09:00.000Z\tTLW.L\t\n" +
                "168\t2014-12-30T03:09:00.000Z\tRRS.L\t\n" +
                "213\t2014-12-30T03:09:00.000Z\tAGK.L\t\n" +
                "214\t2014-12-30T03:09:00.000Z\tBT-A.L\t\n" +
                "204\t2014-12-30T03:09:00.000Z\tLLOY.L\t\n" +
                "214\t2014-12-30T03:09:00.000Z\tWTB.L\t\n" +
                "207\t2014-12-30T03:09:00.000Z\tABF.L\t\n" +
                "185\t2014-12-30T03:10:00.000Z\tBP.L\t\n" +
                "221\t2014-12-30T03:10:00.000Z\tWTB.L\t\n" +
                "206\t2014-12-30T03:10:00.000Z\tLLOY.L\t\n" +
                "215\t2014-12-30T03:10:00.000Z\tBT-A.L\t\n" +
                "195\t2014-12-30T03:10:00.000Z\tTLW.L\t\n" +
                "203\t2014-12-30T03:10:00.000Z\tADM.L\t\n" +
                "220\t2014-12-30T03:10:00.000Z\tAGK.L\t\n" +
                "194\t2014-12-30T03:10:00.000Z\tGKN.L\t\n" +
                "172\t2014-12-30T03:10:00.000Z\tRRS.L\t\n" +
                "189\t2014-12-30T03:10:00.000Z\tABF.L\t\n" +
                "213\t2014-12-30T03:11:00.000Z\tADM.L\t\n" +
                "195\t2014-12-30T03:11:00.000Z\tLLOY.L\t\n" +
                "185\t2014-12-30T03:11:00.000Z\tBP.L\t\n" +
                "198\t2014-12-30T03:11:00.000Z\tBT-A.L\t\n" +
                "210\t2014-12-30T03:11:00.000Z\tRRS.L\t\n" +
                "213\t2014-12-30T03:11:00.000Z\tGKN.L\t\n" +
                "194\t2014-12-30T03:11:00.000Z\tAGK.L\t\n" +
                "220\t2014-12-30T03:11:00.000Z\tWTB.L\t\n" +
                "190\t2014-12-30T03:11:00.000Z\tABF.L\t\n" +
                "182\t2014-12-30T03:11:00.000Z\tTLW.L\t\n" +
                "212\t2014-12-30T03:12:00.000Z\tABF.L\t\n" +
                "214\t2014-12-30T03:12:00.000Z\tAGK.L\t\n" +
                "186\t2014-12-30T03:12:00.000Z\tTLW.L\t\n" +
                "231\t2014-12-30T03:12:00.000Z\tBP.L\t\n" +
                "191\t2014-12-30T03:12:00.000Z\tLLOY.L\t\n" +
                "209\t2014-12-30T03:12:00.000Z\tRRS.L\t\n" +
                "196\t2014-12-30T03:12:00.000Z\tGKN.L\t\n" +
                "191\t2014-12-30T03:12:00.000Z\tADM.L\t\n" +
                "186\t2014-12-30T03:12:00.000Z\tBT-A.L\t\n" +
                "184\t2014-12-30T03:12:00.000Z\tWTB.L\t\n" +
                "4\t2014-12-30T03:13:00.000Z\tBP.L\t\n" +
                "6\t2014-12-30T03:13:00.000Z\tGKN.L\t\n" +
                "7\t2014-12-30T03:13:00.000Z\tTLW.L\t\n" +
                "7\t2014-12-30T03:13:00.000Z\tABF.L\t\n" +
                "6\t2014-12-30T03:13:00.000Z\tADM.L\t\n" +
                "3\t2014-12-30T03:13:00.000Z\tWTB.L\t\n" +
                "5\t2014-12-30T03:13:00.000Z\tRRS.L\t\n" +
                "6\t2014-12-30T03:13:00.000Z\tLLOY.L\t\n" +
                "4\t2014-12-30T03:13:00.000Z\tAGK.L\t\n" +
                "3\t2014-12-30T03:13:00.000Z\tBT-A.L\t\n";


        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 10000, 1419908881558L, 30);
        w.commit();

        int tsIndex = w.getMetadata().getColumnIndex("timestamp");
        int symIndex = w.getMetadata().getColumnIndex("sym");

        MultiMap map = new MultiMap.Builder()
                .keyColumn(w.getMetadata().getColumnMetadata(tsIndex))
                .keyColumn(w.getMetadata().getColumnMetadata(symIndex))
                .valueColumn(new ColumnMetadata() {{
                    name = "count";
                    type = ColumnType.INT;
                }})
                .setCapacity(150)
                .setDataSize(1024 * 1024)
                .setLoadFactor(0.5f)
                .build();

        for (JournalRecord e : w.rows()) {
            long ts = e.getLong(tsIndex);

            MapValues val = map.claimSlot(
                    map.claimKey()
                            .putLong(Dates.floorMI(ts))
                            .putStr(e.getSym(symIndex))
                            .$()
            );

            val.putInt(0, val.isNew() ? 1 : val.getInt(0) + 1);
        }

        RecordSourcePrinter out = new RecordSourcePrinter(sink);
        out.print(map.iterator());
        map.free();

        Assert.assertEquals(expected, sink.toString());

    }
}
