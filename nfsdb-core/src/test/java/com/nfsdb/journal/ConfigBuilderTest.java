/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

import com.nfsdb.journal.factory.configuration.JournalStructure;
import com.nfsdb.journal.lang.cst.JournalEntry;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class ConfigBuilderTest extends AbstractTest {

    @Test
    public void testConfigWithoutClass() throws Exception {
        JournalWriter w = factory.writer(new JournalStructure("test") {{
            $double("bid");
            $double("ask");
            $sym("sym").index();
            $int("bidSize");
            $int("askSize");
            $sym("ex");
            $sym("mode");
            $ts();
        }});
        JournalEntryWriter b;

        b = w.entryWriter(100);
        b.putDouble(0, 10);
        b.putDouble(1, 20);
        b.putSym(2, "BP.L");
        b.append();

        b = w.entryWriter(101);
        b.putDouble(0, 15);
        b.putDouble(1, 45);
        b.putSym(2, "XX.L");
        b.append();

        w.commit();

        String expected = "10.0\t20.0\tBP.L\t0\t0\tnull\tnull\t1970-01-01T00:00:00.100Z\n" +
                "15.0\t45.0\tXX.L\t0\t0\tnull\tnull\t1970-01-01T00:00:00.101Z";

        Journal<MyQuote> r = factory.reader(MyQuote.class, "test");
        TestUtils.assertEquals(expected, r.bufferedIterator());
    }

    @Test
    public void testWriteClassReadGeneric() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 10000);


        Journal r = factory.reader("q");

        Iterator<Quote> expected = w.bufferedIterator();
        JournalSource actual = r.rows();

        while (true) {
            boolean en = expected.hasNext();
            boolean an = actual.hasNext();

            if (en != an) {
                Assert.fail("Sizes of iterators unequal");
            }

            if (!en) {
                break;
            }

            Quote e = expected.next();
            JournalEntry a = actual.next();
            Assert.assertEquals(e.getTimestamp(), a.getLong(0));
            Assert.assertEquals(e.getSym(), a.getSym(1));
            Assert.assertEquals(e.getBid(), a.getDouble(2), 0.0001);
            Assert.assertEquals(e.getAsk(), a.getDouble(3), 0.0001);
            Assert.assertEquals(e.getBidSize(), a.getInt(4));
            Assert.assertEquals(e.getAskSize(), a.getInt(5));
            Assert.assertEquals(e.getMode(), a.getSym(6));
            Assert.assertEquals(e.getEx(), a.getSym(7));
        }
    }

    public static class MyQuote {
        private double bid;
        private double ask;
        private String sym;
        private int bidSize;
        private int askSize;
        private String ex;
        private String mode;
        private long timestamp;
    }
}
