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

import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BulkTest extends AbstractTest {
    @Test
    public void testBulkWrite() throws Exception {
        final int batchSize = 1000000;
        try (JournalWriter<Quote> writer = getWriterFactory().writer(Quote.class)) {
            try (Journal<Quote> reader = getReaderFactory().reader(Quote.class)) {
                reader.setSequentialAccess(true);
                TestUtils.generateQuoteData(writer, batchSize, System.currentTimeMillis(), 12 * 30L * 24L * 60L * 60L * 1000L / batchSize);
                writer.commit();

                Assert.assertTrue(reader.refresh());
                long count = 0;
                for (Quote q : JournalIterators.bufferedIterator(reader)) {
                    assert q != null;
                    count++;
                }
                Assert.assertEquals(batchSize, count);
            }
        }
    }

    @Test
    public void testDurable() throws Exception {
        final int batchSize = 100000;
        final int iterations = 10;
        try (JournalWriter<Quote> writer = getWriterFactory().writer(Quote.class)) {
            try (Journal<Quote> reader = getReaderFactory().reader(Quote.class)) {
                reader.setSequentialAccess(true);
                long start = System.currentTimeMillis();
                long p = 10L * 24L * 60L * 60L * 1000L;
                for (int i = 0; i < iterations; i++) {
                    TestUtils.generateQuoteData(writer, batchSize, start, p / batchSize);
                    writer.commitDurable();
                    start += p;
                }

                Assert.assertTrue(reader.refresh());

                long count = 0;
                for (Quote q : JournalIterators.bufferedIterator(reader)) {
                    assert q != null;
                    count++;
                }
                Assert.assertEquals(batchSize * iterations, count);
            }
        }
    }
}
