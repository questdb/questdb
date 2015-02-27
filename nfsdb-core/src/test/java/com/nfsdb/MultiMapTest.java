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

package com.nfsdb;

import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.column.ColumnType;
import com.nfsdb.exp.CharSink;
import com.nfsdb.exp.RecordSourcePrinter;
import com.nfsdb.exp.StringSink;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class MultiMapTest extends AbstractTest {

    private final CharSink sink = new StringSink();

    @Test
    public void testCount() throws Exception {

        final String expected = "186\t2014-12-30T03:08:00.000Z\tAGK.L\n" +
                "185\t2014-12-30T03:08:00.000Z\tBP.L\n" +
                "216\t2014-12-30T03:08:00.000Z\tRRS.L\n" +
                "196\t2014-12-30T03:08:00.000Z\tBT-A.L\n" +
                "214\t2014-12-30T03:08:00.000Z\tGKN.L\n" +
                "184\t2014-12-30T03:08:00.000Z\tLLOY.L\n" +
                "187\t2014-12-30T03:08:00.000Z\tABF.L\n" +
                "196\t2014-12-30T03:08:00.000Z\tWTB.L\n" +
                "193\t2014-12-30T03:08:00.000Z\tTLW.L\n" +
                "192\t2014-12-30T03:08:00.000Z\tADM.L\n" +
                "189\t2014-12-30T03:09:00.000Z\tBP.L\n" +
                "203\t2014-12-30T03:09:00.000Z\tGKN.L\n" +
                "201\t2014-12-30T03:09:00.000Z\tADM.L\n" +
                "187\t2014-12-30T03:09:00.000Z\tTLW.L\n" +
                "168\t2014-12-30T03:09:00.000Z\tRRS.L\n" +
                "213\t2014-12-30T03:09:00.000Z\tAGK.L\n" +
                "214\t2014-12-30T03:09:00.000Z\tBT-A.L\n" +
                "204\t2014-12-30T03:09:00.000Z\tLLOY.L\n" +
                "214\t2014-12-30T03:09:00.000Z\tWTB.L\n" +
                "207\t2014-12-30T03:09:00.000Z\tABF.L\n" +
                "185\t2014-12-30T03:10:00.000Z\tBP.L\n" +
                "221\t2014-12-30T03:10:00.000Z\tWTB.L\n" +
                "206\t2014-12-30T03:10:00.000Z\tLLOY.L\n" +
                "215\t2014-12-30T03:10:00.000Z\tBT-A.L\n" +
                "195\t2014-12-30T03:10:00.000Z\tTLW.L\n" +
                "203\t2014-12-30T03:10:00.000Z\tADM.L\n" +
                "220\t2014-12-30T03:10:00.000Z\tAGK.L\n" +
                "194\t2014-12-30T03:10:00.000Z\tGKN.L\n" +
                "172\t2014-12-30T03:10:00.000Z\tRRS.L\n" +
                "189\t2014-12-30T03:10:00.000Z\tABF.L\n" +
                "213\t2014-12-30T03:11:00.000Z\tADM.L\n" +
                "195\t2014-12-30T03:11:00.000Z\tLLOY.L\n" +
                "185\t2014-12-30T03:11:00.000Z\tBP.L\n" +
                "198\t2014-12-30T03:11:00.000Z\tBT-A.L\n" +
                "210\t2014-12-30T03:11:00.000Z\tRRS.L\n" +
                "213\t2014-12-30T03:11:00.000Z\tGKN.L\n" +
                "194\t2014-12-30T03:11:00.000Z\tAGK.L\n" +
                "220\t2014-12-30T03:11:00.000Z\tWTB.L\n" +
                "190\t2014-12-30T03:11:00.000Z\tABF.L\n" +
                "182\t2014-12-30T03:11:00.000Z\tTLW.L\n" +
                "212\t2014-12-30T03:12:00.000Z\tABF.L\n" +
                "214\t2014-12-30T03:12:00.000Z\tAGK.L\n" +
                "186\t2014-12-30T03:12:00.000Z\tTLW.L\n" +
                "231\t2014-12-30T03:12:00.000Z\tBP.L\n" +
                "191\t2014-12-30T03:12:00.000Z\tLLOY.L\n" +
                "209\t2014-12-30T03:12:00.000Z\tRRS.L\n" +
                "196\t2014-12-30T03:12:00.000Z\tGKN.L\n" +
                "191\t2014-12-30T03:12:00.000Z\tADM.L\n" +
                "186\t2014-12-30T03:12:00.000Z\tBT-A.L\n" +
                "184\t2014-12-30T03:12:00.000Z\tWTB.L\n" +
                "4\t2014-12-30T03:13:00.000Z\tBP.L\n" +
                "6\t2014-12-30T03:13:00.000Z\tGKN.L\n" +
                "7\t2014-12-30T03:13:00.000Z\tTLW.L\n" +
                "7\t2014-12-30T03:13:00.000Z\tABF.L\n" +
                "6\t2014-12-30T03:13:00.000Z\tADM.L\n" +
                "3\t2014-12-30T03:13:00.000Z\tWTB.L\n" +
                "5\t2014-12-30T03:13:00.000Z\tRRS.L\n" +
                "6\t2014-12-30T03:13:00.000Z\tLLOY.L\n" +
                "4\t2014-12-30T03:13:00.000Z\tAGK.L\n" +
                "3\t2014-12-30T03:13:00.000Z\tBT-A.L\n";


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
                            .putInt(e.getInt(symIndex))
                            .commit()
            );

            val.putInt(0, val.isNew() ? 1 : val.getInt(0) + 1);
        }

        RecordSourcePrinter out = new RecordSourcePrinter(sink);
        out.print(map.getRecordSource());
        map.free();

        Assert.assertEquals(expected, sink.toString());

    }
}
