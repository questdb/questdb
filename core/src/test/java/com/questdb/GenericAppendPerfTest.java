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

import com.questdb.factory.configuration.JournalStructure;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Dates;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class GenericAppendPerfTest extends AbstractTest {

    private static final int TEST_DATA_SIZE = 2000000;
    private static final Log LOG = LogFactory.getLog(GenericAppendPerfTest.class);

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
        LOG.info().$("generic append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result) / 2).$("ms").$();
    }
}
