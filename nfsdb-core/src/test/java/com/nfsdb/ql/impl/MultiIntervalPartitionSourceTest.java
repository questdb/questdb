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

package com.nfsdb.ql.impl;

import com.nfsdb.JournalWriter;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StdoutSink;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import org.junit.Test;

public class MultiIntervalPartitionSourceTest extends AbstractTest {

    @Test
    public void testIntervalMerge() throws Exception {

        JournalWriter<Quote> w = factory.writer(Quote.class);

        TestUtils.generateQuoteData(w, 600, Dates.parseDateTime("2014-03-10T02:00:00.000Z"), Dates.MINUTE_MILLIS);
        w.commit();
        RecordSourcePrinter p = new RecordSourcePrinter(new StdoutSink());

        p.print(
                new JournalSource(
                        new MultiIntervalPartitionSource(
                                new JournalPartitionSource(w, true),
                                new MillisIntervalSource(
                                        new Interval("2014-03-10T07:00:00.000Z", "2014-03-10T07:15:00.000Z"),
                                        30 * Dates.MINUTE_MILLIS,
                                        5
                                )
                        ),
                        new AllRowSource()
                )
        );

    }
}