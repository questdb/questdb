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


import com.nfsdb.exceptions.JournalException;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class NullTest extends AbstractTest {

    @Test
    public void tumbleTryNullTest() throws JournalException {
        final int TEST_DATA_SIZE = (int) 1E3;
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 1000);
        long timestamp = Dates.parseDateTime("2013-10-05T10:00:00.000Z");
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
