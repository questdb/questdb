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

import com.questdb.misc.Dates;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class EmptyJournalTest extends AbstractTest {

    @Test
    public void testEmptyJournalIterator() throws Exception {
        try (JournalWriter w = getWriterFactory().writer(Quote.class)) {
            testJournalIterator(w);
        }
    }

    @Test
    public void testJournalWithEmptyPartition() throws Exception {
        try (JournalWriter<Quote> w = getWriterFactory().writer(Quote.class)) {
            w.getAppendPartition(Dates.parseDateTime("2012-02-10T10:00:00.000Z"));
            w.getAppendPartition(Dates.parseDateTime("2012-03-10T10:00:00.000Z"));
            testJournalIterator(w);
        }
    }

    private void testJournalIterator(Journal journal) throws Exception {
        Assert.assertFalse(journal.iterator().hasNext());
    }
}
