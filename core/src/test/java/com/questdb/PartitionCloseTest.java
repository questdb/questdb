/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.model.Quote;
import com.questdb.store.Journal;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionCloseTest extends AbstractTest {

    @Test
    public void testCommitFileClose() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {

            Quote q = new Quote();
            q.setBid(10);
            w.append(q);
            Thread.sleep(1000);
            w.commit();
            w.append(q);
        }

        try (Journal<Quote> r = getFactory().reader(Quote.class)) {
            Assert.assertEquals(2, r.size());
        }
    }
}
