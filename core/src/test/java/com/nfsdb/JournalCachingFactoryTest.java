/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.factory.JournalCachingFactory;
import com.nfsdb.misc.Files;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
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
