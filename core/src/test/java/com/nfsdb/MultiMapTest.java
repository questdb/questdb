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

package com.nfsdb;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.impl.CollectionRecordMetadata;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
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


        final JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 10000, 1419908881558L, 30);
        w.commit();

        final int tsIndex = w.getMetadata().getColumnIndex("timestamp");
        final int symIndex = w.getMetadata().getColumnIndex("sym");

        CollectionRecordMetadata keyMeta = new CollectionRecordMetadata()
                .add(w.getMetadata().getColumn(tsIndex))
                .add(w.getMetadata().getColumn(symIndex));
        MultiMap map = new MultiMap(
                keyMeta,
                keyMeta.getColumnNames(),
                new ObjList<RecordColumnMetadata>() {{
                    add(new ColumnMetadata() {{
                        name = "count";
                        type = ColumnType.INT;
                    }});
                }},
                null);

        for (Record e : compiler.compile(factory, "quote")) {
            long ts = e.getLong(tsIndex);

            MapValues val = map.getOrCreateValues(
                    map.keyWriter()
                            .putLong(Dates.floorMI(ts))
                            .putInt(e.getInt(symIndex))
            );

            val.putInt(0, val.isNew() ? 1 : val.getInt(0) + 1);
        }

        RecordSourcePrinter out = new RecordSourcePrinter(sink);
        RecordCursor c = map.getCursor();
        out.printCursor(c);
        map.free();

        Assert.assertEquals(expected, sink.toString());

    }
}
