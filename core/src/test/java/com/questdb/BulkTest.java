/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb;

import com.questdb.factory.JournalFactory;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
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
        for (Quote q : JournalIterators.bufferedIterator(reader)) {
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
        for (Quote q : JournalIterators.bufferedIterator(reader)) {
            assert q != null;
            count++;
        }
        Assert.assertEquals(batchSize * iterations, count);
    }
}
