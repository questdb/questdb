/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class EmptyJournalTest extends AbstractTest {

    @Test
    public void testEmptyJournalIterator() throws Exception {
        testJournalIterator(factory.writer(Quote.class));
    }

    @Test
    public void testJournalWithEmptyPartition() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.getAppendPartition(Dates.parseDateTime("2012-02-10T10:00:00.000Z"));
        w.getAppendPartition(Dates.parseDateTime("2012-03-10T10:00:00.000Z"));
        testJournalIterator(w);
    }

    private void testJournalIterator(Journal journal) throws Exception {
        Assert.assertFalse(journal.iterator().hasNext());
    }
}
