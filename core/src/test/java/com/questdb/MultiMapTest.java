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

package com.questdb;

import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Dates;
import com.questdb.model.Quote;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
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
