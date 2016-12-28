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

import com.questdb.misc.Rows;
import com.questdb.model.Quote;
import com.questdb.store.Tx;
import com.questdb.store.TxLog;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import com.questdb.txt.sink.DelimitedCharSink;
import com.questdb.txt.sink.StringSink;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class TxLogTest extends AbstractTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testTx() throws Exception {
        File dir = temp.newFolder();
        try (TxLog txLog = new TxLog(dir, JournalMode.APPEND, 2)) {
            Assert.assertFalse(txLog.hasNext());

            Tx tx = new Tx();
            tx.prevTxAddress = 99999;
            tx.command = 0;
            tx.journalMaxRowID = 10;
            tx.lagSize = 12;
            tx.lagName = "abcrrrrrrrrrrrrrrrrrrrrrrrrrrr";
            tx.timestamp = 1000001L;
            tx.lastPartitionTimestamp = 200002L;
            tx.symbolTableSizes = new int[]{10, 12};
            tx.symbolTableIndexPointers = new long[]{2, 15, 18};
            tx.indexPointers = new long[]{36, 48};
            tx.lagIndexPointers = new long[]{55, 67};

            txLog.write(tx, true);

            Assert.assertFalse(txLog.hasNext());

            try (TxLog r = new TxLog(dir, JournalMode.READ, 2)) {
                Assert.assertTrue(r.hasNext());

                Tx tx1 = new Tx();
                r.head(tx1);
                Assert.assertEquals(0, tx1.command);
                Assert.assertEquals(10, tx1.journalMaxRowID);
                Assert.assertEquals(12, tx1.lagSize);
                Assert.assertEquals("abcrrrrrrrrrrrrrrrrrrrrrrrrrrr", tx.lagName);
                Assert.assertEquals(99999, tx.prevTxAddress);
                Assert.assertEquals(1000001L, tx.timestamp);
                Assert.assertEquals(200002L, tx.lastPartitionTimestamp);
                Assert.assertEquals(1000001L, tx.timestamp);

                Assert.assertArrayEquals(new int[]{10, 12}, tx.symbolTableSizes);
                Assert.assertArrayEquals(new long[]{2, 15, 18}, tx.symbolTableIndexPointers);
                Assert.assertArrayEquals(new long[]{36, 48}, tx.indexPointers);
                Assert.assertArrayEquals(new long[]{55, 67}, tx.lagIndexPointers);

                Assert.assertFalse(r.hasNext());
            }
        }
    }

    @Test
    public void testTxLogIterator() throws Exception {
        try (JournalWriter<Quote> w = getWriterFactory().writer(Quote.class)) {
            for (int i = 0; i < 10; i++) {
                TestUtils.generateQuoteData(w, 100, w.getMaxTimestamp());
                w.commit();
            }


            final String expected = "10\t0\t1000\n" +
                    "9\t0\t900\n" +
                    "8\t0\t800\n" +
                    "7\t0\t700\n" +
                    "6\t0\t600\n" +
                    "5\t0\t500\n" +
                    "4\t0\t400\n" +
                    "3\t0\t300\n" +
                    "2\t0\t200\n" +
                    "1\t0\t100\n" +
                    "0\t0\t0\n";

            DelimitedCharSink sink = new DelimitedCharSink(new StringSink(), '\t', "\n");
            for (Tx tx : w.transactions()) {
                sink.put(tx.txn);
                sink.put(tx.journalMaxRowID == -1 ? 0 : Rows.toPartitionIndex(tx.journalMaxRowID));
                sink.put(tx.journalMaxRowID == -1 ? 0 : Rows.toLocalRowID(tx.journalMaxRowID));
                sink.eol();
            }
            sink.flush();
            Assert.assertEquals(expected, sink.toString());
        }
    }
}

