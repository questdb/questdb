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

import com.questdb.factory.JournalCachingFactory;
import com.questdb.misc.Files;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
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
