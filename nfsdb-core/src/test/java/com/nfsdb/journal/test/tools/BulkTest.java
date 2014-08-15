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

package com.nfsdb.journal.test.tools;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.model.Quote;
import org.junit.Assert;
import org.junit.Test;

public class BulkTest extends AbstractTest {
    @Test
    public void testBulkWrite() throws Exception {
        final int batchSize = 1000000;
        JournalWriter<Quote> writer = factory.bulkWriter(Quote.class);
        Journal<Quote> reader = factory.bulkReader(Quote.class);

        TestUtils.generateQuoteData(writer, batchSize, System.currentTimeMillis(), 12 * 30L * 24L * 60L * 60L * 1000L / batchSize);
        writer.commit();

        Assert.assertTrue(reader.refresh());
        long count = 0;
        for (Quote q : reader.bufferedIterator()) {
            assert q != null;
            count++;
        }
        Assert.assertEquals(batchSize, count);
    }

    @Test
    public void testDurable() throws Exception {
        JournalFactory f = factory;
        final int batchSize = 100000;
        final int iterations = 10;
        JournalWriter<Quote> writer = f.bulkWriter(Quote.class);
        Journal<Quote> reader = f.bulkReader(Quote.class);


        long start = System.currentTimeMillis();
        long p = 10L * 24L * 60L * 60L * 1000L;
        for (int i = 0; i < iterations; i++) {
            TestUtils.generateQuoteData(writer, batchSize, start, p / batchSize);
            writer.commitDurable();
            start += p;
        }

        Assert.assertTrue(reader.refresh());

        long count = 0;
        for (Quote q : reader.bufferedIterator()) {
            assert q != null;
            count++;
        }
        Assert.assertEquals(batchSize * iterations, count);
    }
}
