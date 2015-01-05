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

import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.tx.Tx;
import com.nfsdb.tx.TxLog;
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
        TxLog txLog = new TxLog(dir, JournalMode.APPEND);
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

        txLog.create(tx);

        Assert.assertFalse(txLog.hasNext());

        TxLog r = new TxLog(dir, JournalMode.READ);
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
        txLog.close();
        r.close();
    }

    @Test
    public void testTxLogWalk() throws Exception {
        JournalWriter<Quote> writer = factory.writer(Quote.class);
        // tx1
        TestUtils.generateQuoteData(writer, 100, System.currentTimeMillis());
        writer.commit();
        // tx2
        TestUtils.generateQuoteData(writer, 100, System.currentTimeMillis());
        writer.commit();
        // tx3
        TestUtils.generateQuoteData(writer, 100, System.currentTimeMillis());
        writer.commit();
        // tx4
        TestUtils.generateQuoteData(writer, 100, System.currentTimeMillis());
        writer.commit();

        Assert.assertEquals(400, writer.size());
        writer.rollback(writer.txLog.prevAddress(writer.txLog.headAddress()));
        Assert.assertEquals(300, writer.size());

        Tx tx = new Tx();
        long address = writer.txLog.headAddress();
        writer.txLog.get(address, tx);
        Assert.assertEquals(300, tx.journalMaxRowID);
    }
}

