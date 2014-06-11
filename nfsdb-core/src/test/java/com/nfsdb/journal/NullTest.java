package com.nfsdb.journal;


import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class NullTest extends AbstractTest {

    @Test
    public void tumbleTryNullTest() throws JournalException {
        final int TEST_DATA_SIZE = (int) 1E3;
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 1000);
        long timestamp = Dates.toMillis("2013-10-05T10:00:00.000Z");
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        Quote q = new Quote();
        int increment = 6000;

        for (int i = 0; i < TEST_DATA_SIZE; i++) {
            q.clear();

            if (i % 7 != 0) {
                q.setSym(symbols[i % symbols.length]);
            }

            if (i % 11 != 0) {
                q.setAsk(i * 22.98007E8);
            }

            if (i % 13 != 0) {
                q.setBid(i * 22.98007E-8);
            }

            if (i % 3 != 0) {
                q.setAskSize(i);
            }

            if (i % 5 != 0) {
                q.setBidSize(i * 7);
            }

            if (i % 2 != 0) {
                q.setEx("LXE");
            }

            if (i % 17 != 0) {
                q.setMode("Some interesting string with киррилица and special char" + (char) (i % Character.MAX_VALUE));
            }

            q.setTimestamp(timestamp);
            timestamp += increment;
            w.append(q);
        }

        w.commit();
        w.close();

        Journal<Quote> r = factory.reader(Quote.class, "quote");
        int i = 0;
        for (Quote qr : r.bufferedIterator()) {
            if (i % 7 != 0) {
                Assert.assertEquals(symbols[i % symbols.length], qr.getSym());
            }

            if (i % 11 != 0) {
                Assert.assertEquals(i * 22.98007E8, qr.getAsk(), 1E-9);
            }

            if (i % 13 != 0) {
                Assert.assertEquals(i * 22.98007E-8, qr.getBid(), 1E-9);
            }

            if (i % 3 != 0) {
                Assert.assertEquals(i, qr.getAskSize());
            }

            if (i % 5 != 0) {
                Assert.assertEquals(i * 7, qr.getBidSize());
            }

            if (i % 2 != 0) {
                Assert.assertEquals("LXE", qr.getEx());
            }

            if (i % 17 != 0) {
                Assert.assertEquals("Some interesting string with киррилица and special char" + (char) (i % Character.MAX_VALUE), qr.getMode());
            }
            i++;
        }

    }
}
