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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ArrayAsMapKeyTest extends AbstractCairoTest {

    @Test
    public void testArrayAsGroupByKey() throws Exception {
        execute("create table array_test(k symbol, ob_buy double[][], ob_sell double[][], ts timestamp) timestamp(ts) partition by day ;");
        execute(
                "insert into array_test values \n" +
                        "   ('vod', ARRAY[[9., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),\n" +
                        "   ('vod2', ARRAY[[4., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),   \n" +
                        "   ('vod3', ARRAY[[3., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123)\n" +
                        "   ;\n"
        );
        assertQuery("[]\tk\tcount\n" +
                        "[[9.0,1000.0],[10.0,10000.0]]\tvod\t1\n" +
                        "[[4.0,1000.0],[10.0,10000.0]]\tvod2\t1\n" +
                        "[[3.0,1000.0],[10.0,10000.0]]\tvod3\t1\n",
                "select ob_buy[1:], k, count() from array_test;",
                true,
                true
        );

        assertQuery("ob_buy\tk\tcount\n" +
                        "[[9.0,1000.0],[10.0,10000.0]]\tvod\t1\n" +
                        "[[4.0,1000.0],[10.0,10000.0]]\tvod2\t1\n" +
                        "[[3.0,1000.0],[10.0,10000.0]]\tvod3\t1\n",
                "select ob_buy, k, count() from array_test;",
                true,
                true
        );
    }

    @Test
    public void testArrayAsOrderByColumn() throws Exception {
        execute("create table array_test(k symbol, ob_buy double[][], ob_sell double[][], ts timestamp) timestamp(ts) partition by day ;");
        execute(
                "insert into array_test values \n" +
                        "   ('vod', ARRAY[[9., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),\n" +
                        "   ('vod2', ARRAY[[4., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),   \n" +
                        "   ('vod3', ARRAY[[3., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123)\n" +
                        "   ;\n"
        );

        assertException(
                "select ob_buy[1:] c, k from array_test order by c;",
                48,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );

        assertException(
                "select ob_buy, k from array_test order by ob_buy;",
                42,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );

        assertException(
                "select k, ob_buy from array_test order by 2;",
                42,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );
    }
}
