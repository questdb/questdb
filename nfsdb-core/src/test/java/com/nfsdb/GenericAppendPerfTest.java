/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.logging.Logger;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class GenericAppendPerfTest extends AbstractTest {

    public static final int TEST_DATA_SIZE = 2000000;
    private static final Logger LOGGER = Logger.getLogger(GenericAppendPerfTest.class);

    @Test
    public void testAppend() throws Exception {


        JournalWriter wg = factory.writer(
                new JournalStructure("qq") {{
                    $sym("sym").index().valueCountHint(20);
                    $double("bid");
                    $double("ask");
                    $int("bidSize");
                    $int("askSize");
                    $sym("ex").valueCountHint(1);
                    $sym("mode").valueCountHint(1);
                    recordCountHint(TEST_DATA_SIZE);
                }}
        );

        long t = System.nanoTime();
        TestUtils.generateQuoteDataGeneric(wg, TEST_DATA_SIZE, Dates.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
        wg.commit();
        long result = System.nanoTime() - t;
        LOGGER.info("generic append (1M): " + TimeUnit.NANOSECONDS.toMillis(result) / 2 + "ms");
    }
}
