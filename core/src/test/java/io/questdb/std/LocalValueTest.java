/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class LocalValueTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCloseable() {
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
    public void testLocalValue() {
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
        public void close() {
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