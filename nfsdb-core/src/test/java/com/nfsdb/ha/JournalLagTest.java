/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.ha;

import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JournalLagTest extends AbstractJournalTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T00:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T01:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T13:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-05-01T00:00:00.000Z"), 100);
    }

    @Test
    public void testLagDetach() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 500));
        master.mergeAppend(origin.query().all().asResultSet().subset(500, 600));
        master.commit();

        executeSequence(true);
        master.removeIrregularPartition();
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testLagOnlyPropagation() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 500));
        master.mergeAppend(origin.query().all().asResultSet().subset(500, 600));
        master.commit();
        String lagName = master.getIrregularPartition().getName();

        executeSequence(true);

        master.mergeAppend(origin.query().all().asResultSet().subset(600, 700));
        master.commit();
        Assert.assertEquals(lagName, master.getIrregularPartition().getName());

        executeSequence(true);
    }
}
