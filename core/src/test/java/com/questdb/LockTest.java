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

import com.questdb.ex.JournalException;
import com.questdb.factory.ReaderFactory;
import com.questdb.misc.Dates;
import com.questdb.model.Quote;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class LockTest extends AbstractTest {

    @Test
    public void testLockAcrossClassLoaders() throws JournalException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        URLClassLoader classLoader = new URLClassLoader(((URLClassLoader) this.getClass().getClassLoader()).getURLs(), null);

        JournalWriter<Quote> rw = getWriterFactory().writer(Quote.class);
        rw.close();
        rw.delete();

        rw = getWriterFactory().writer(Quote.class);

        List<Quote> data = new ArrayList<>();
        data.add(new Quote().setSym("S1").setTimestamp(Dates.toMillis(2013, 3, 10, 15, 0)));
        data.add(new Quote().setSym("S2").setTimestamp(Dates.toMillis(2013, 3, 10, 16, 0)));
        rw.mergeAppend(data);
        rw.commit();

        new TestAccessor(getReaderFactory().getConfiguration().getJournalBase());
        classLoader.loadClass("com.questdb.LockTest$TestAccessor").getConstructor(File.class)
                .newInstance(getReaderFactory().getConfiguration().getJournalBase());

        rw.close();
        rw.delete();
    }

    public static class TestAccessor {
        public TestAccessor(File journalBase) throws JournalException {
            try (ReaderFactory factory = new ReaderFactory(ModelConfiguration.MAIN.build(journalBase))) {
                Journal<Quote> reader = factory.reader(Quote.class);
                Assert.assertEquals(2, reader.size());
            }
        }
    }
}
