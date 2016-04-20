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

package com.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

public class LocalValueTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCloseable() throws Exception {
        LocalValue<Closeable>[] values = new LocalValue[1024];
        ClosableImpl[] closeables = new ClosableImpl[values.length];

        LocalityImpl locality = new LocalityImpl();
        for (int i = 0; i < values.length; i++) {
            values[i] = new LocalValue<>();
            values[i].set(locality, closeables[i] = new ClosableImpl());
        }

        locality.clear();

        for (int i = 0; i < values.length; i++) {
            Assert.assertNull(values[i].get(locality));
            Assert.assertTrue(closeables[i].closed);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLocalValue() throws Exception {
        LocalValue<Integer>[] values = new LocalValue[512 * 1024];

        Locality locality1 = new LocalityImpl();
        Locality locality2 = new LocalityImpl();

        for (int i = 0; i < values.length; i++) {
            values[i] = new LocalValue<>();
            values[i].set(locality1, i);
            values[i].set(locality2, i + 10000000);
        }

        for (int i = 0; i < values.length; i++) {
            Assert.assertEquals((int) values[i].get(locality1), i);
            Assert.assertEquals((int) values[i].get(locality2), i + 10000000);
        }
    }

    private static class ClosableImpl implements Closeable {
        private boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    private static class LocalityImpl implements Locality {
        private final LocalValueMap map = new LocalValueMap();

        public void clear() {
            map.close();
        }

        @Override
        public LocalValueMap getMap() {
            return map;
        }
    }
}