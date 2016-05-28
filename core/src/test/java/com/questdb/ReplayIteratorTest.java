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

import com.questdb.iter.ReplayIterator;
import com.questdb.iter.TimeSource;
import com.questdb.iter.clock.Clock;
import com.questdb.iter.clock.MilliClock;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReplayIteratorTest extends AbstractTest {
    @Test
    public void testJournalIteratorReplay() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        ReplayIterator<Quote> replay = new ReplayIterator<>(JournalIterators.bufferedIterator(w), 0.00000001f);
        TestUtils.assertEquals(JournalIterators.bufferedIterator(w), replay);
    }

    @Test
    public void testJournalReplay() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        ReplayIterator<Quote> replay = new ReplayIterator<>(w, 0.00000001f);
        TestUtils.assertEquals(JournalIterators.bufferedIterator(w), replay);
    }

    @Test
    public void testReplay() throws Exception {

        Clock clock = MilliClock.INSTANCE;

        final long t = clock.getTicks();

        List<Entity> entities = new ArrayList<Entity>() {{
            add(new Entity(t));
            add(new Entity(t + 50));
            add(new Entity(t + 65));
            add(new Entity(t + 250));
            add(new Entity(t + 349));
        }};

        long expected[] = deltas(entities);

        ReplayIterator<Entity> replay = new ReplayIterator<>(entities.iterator(), clock, 1f, new TimeSource<Entity>() {
            @Override
            public long getTicks(Entity object) {
                return object.timestamp;
            }
        });

        List<Entity> list = new ArrayList<>();

        for (Entity e : replay) {
            if (e.timestamp > 0) {
                e.timestamp = clock.getTicks();
            }
            list.add(e);
        }

        long actual[] = deltas(list);

        Assert.assertArrayEquals(expected, actual);

    }

    private long[] deltas(List<Entity> entities) {
        long last = 0;
        long result[] = new long[entities.size()];

        for (int i = 0; i < entities.size(); i++) {
            Entity e = entities.get(i);
            result[i] = last == 0 ? 0 : e.timestamp - last;
            last = e.timestamp;
        }

        return result;
    }

    private static class Entity {
        private long timestamp;

        private Entity(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
