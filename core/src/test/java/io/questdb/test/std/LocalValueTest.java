/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.std;

import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.LocalValueMap;
import io.questdb.cutlass.http.Locality;
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