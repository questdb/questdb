/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store;


import com.questdb.model.Quote;
import com.questdb.std.NumericException;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class NullTest extends AbstractTest {

    @Test
    public void tumbleDryNullTest() throws JournalException, NumericException {
        final int TEST_DATA_SIZE = (int) 1E3;
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", 1000)) {
            long timestamp = DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z");
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


            try (Journal<Quote> r = getFactory().reader(Quote.class, "quote")) {
                int i = 0;
                for (Quote qr : JournalIterators.bufferedIterator(r)) {
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
    }
}
