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

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.test.tools.AbstractTest;
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

        JournalWriter<Quote> rw = factory.writer(Quote.class);
        rw.close();
        rw.delete();

        rw = factory.writer(Quote.class);

        List<Quote> data = new ArrayList<>();
        data.add(new Quote().setSym("S1").setTimestamp(Dates.toMillis(2013, 3, 10, 15, 0)));
        data.add(new Quote().setSym("S2").setTimestamp(Dates.toMillis(2013, 3, 10, 16, 0)));
        rw.mergeAppend(data);
        rw.commit();

        new TestAccessor(factory.getConfiguration().getJournalBase());
        classLoader.loadClass("com.nfsdb.LockTest$TestAccessor").getConstructor(File.class)
                .newInstance(factory.getConfiguration().getJournalBase());

        rw.close();
        rw.delete();
    }

    public static class TestAccessor {
        public TestAccessor(File journalBase) throws JournalException {
            JournalFactory factory = new JournalFactory(ModelConfiguration.MAIN.build(journalBase));
            Journal<Quote> reader = factory.reader(Quote.class);
            Assert.assertEquals(2, reader.size());
            reader.close();
        }
    }
}
