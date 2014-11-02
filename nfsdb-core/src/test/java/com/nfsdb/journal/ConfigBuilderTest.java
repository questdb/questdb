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

import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Iterator;

public class ConfigBuilderTest extends AbstractTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testConfigWithoutClass() throws Exception {
        JournalConfigurationBuilder builder = new JournalConfigurationBuilder() {{
            $("test")
                    .$double("bid")
                    .$double("ask")

            ;
        }};

        JournalFactory factory = new JournalFactory(builder.build(temp.newFolder()));
        JournalWriter w = factory.writer(new JournalKey<>("test"));
        System.out.println(w.getLocation());

        Journal<Quote> r = factory.reader(Quote.class, "test");
        System.out.println(r.getLocation());
        System.out.println(r.getMetadata());
    }

    @Test
    public void testWriteClassReadGeneric() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 10000);
        Journal r = factory.reader(Quote.class.getName());

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
            DataItem a = actual.next();
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
}
