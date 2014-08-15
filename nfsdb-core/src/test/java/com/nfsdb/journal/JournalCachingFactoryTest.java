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

import com.nfsdb.journal.factory.JournalCachingFactory;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.Test;

public class JournalCachingFactoryTest extends AbstractTest {

    @Test
    public void testFactory() throws Exception {

        // create journals so there is something to read.
        factory.writer(Quote.class);
        factory.writer(Quote.class, "loc");

        JournalCachingFactory cachingFactory = new JournalCachingFactory(factory.getConfiguration());

        // create first reader
        Journal<Quote> reader = cachingFactory.reader(Quote.class);
        Assert.assertNotNull(reader);

        // check that reader doesn't close
        reader.close();
        Assert.assertTrue(reader.isOpen());

        // check that repeated reader is the same instance
        Journal<Quote> reader2 = cachingFactory.reader(Quote.class);
        Assert.assertSame(reader, reader2);

        // get reader at different location
        Journal<Quote> reader3 = cachingFactory.reader(Quote.class, "loc");
        Assert.assertNotSame(reader, reader3);

        Journal<Quote> reader4 = cachingFactory.reader(Quote.class, "loc");
        Assert.assertSame(reader3, reader4);

        cachingFactory.close();
        Files.delete(cachingFactory.getConfiguration().getJournalBase());
    }
}
