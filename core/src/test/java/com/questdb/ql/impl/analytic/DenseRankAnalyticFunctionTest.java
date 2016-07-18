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

package com.questdb.ql.impl.analytic;

import com.questdb.ex.ParserException;
import com.questdb.ql.parser.QueryError;
import com.questdb.test.tools.TestUtils;
import org.junit.Test;

public class DenseRankAnalyticFunctionTest extends AbstractAnalyticRecordSourceTest {
    @Test
    public void testRank() throws Exception {
        assertThat("5\ttrue\tAX\t2016-05-01T10:26:00.000Z\n" +
                        "6\tfalse\tBZ\t2016-05-01T10:27:00.000Z\n" +
                        "7\ttrue\tBZ\t2016-05-01T10:28:00.000Z\n" +
                        "8\tfalse\tAX\t2016-05-01T10:29:00.000Z\n" +
                        "9\tfalse\tBZ\t2016-05-01T10:30:00.000Z\n",
                "select dense_rank() x over(), boo, str, timestamp from 'abc' limit 5,10");
    }

    @Test
    public void testRankOrdered() throws Exception {
        assertThat("15\tBZ\t1.050231933594\n" +
                        "17\tXX\t566.734375000000\n" +
                        "9\tKK\t0.000013792171\n" +
                        "6\tAX\t0.000000567185\n" +
                        "1\tAX\t-512.000000000000\n" +
                        "14\tAX\t0.675451681018\n" +
                        "13\tBZ\t0.332301996648\n" +
                        "7\tBZ\t0.000001752813\n" +
                        "11\tAX\t0.000076281818\n" +
                        "2\tBZ\t0.000000005555\n" +
                        "8\tXX\t0.000002473130\n" +
                        "18\tKK\t632.921875000000\n" +
                        "4\tAX\t0.000000020896\n" +
                        "12\tBZ\t0.007371325744\n" +
                        "3\tXX\t0.000000014643\n" +
                        "16\tAX\t512.000000000000\n" +
                        "19\tXX\t864.000000000000\n" +
                        "5\tAX\t0.000000157437\n" +
                        "0\tBZ\t-842.000000000000\n" +
                        "10\tBZ\t0.000032060649\n",
                "select dense_rank() x over(order by d), str, d from abc");
    }

    @Test
    public void testRankOrderedPartitioned() throws Exception {
        assertThat("BZ\t0\n" +
                        "XX\t0\n" +
                        "KK\t0\n" +
                        "AX\t0\n" +
                        "AX\t1\n" +
                        "AX\t2\n" +
                        "BZ\t1\n" +
                        "BZ\t2\n" +
                        "AX\t3\n" +
                        "BZ\t3\n" +
                        "XX\t1\n" +
                        "KK\t1\n" +
                        "AX\t4\n" +
                        "BZ\t4\n" +
                        "XX\t2\n" +
                        "AX\t5\n" +
                        "XX\t3\n" +
                        "AX\t6\n" +
                        "BZ\t5\n" +
                        "BZ\t6\n",
                "select str, dense_rank() rank over(partition by str order by timestamp) from 'abc'");
    }

    @Test
    public void testRankPartitioned() throws Exception {
        assertThat("BZ\t0\n" +
                        "XX\t0\n" +
                        "KK\t0\n" +
                        "AX\t0\n" +
                        "AX\t1\n" +
                        "AX\t2\n" +
                        "BZ\t1\n" +
                        "BZ\t2\n" +
                        "AX\t3\n" +
                        "BZ\t3\n" +
                        "XX\t1\n" +
                        "KK\t1\n" +
                        "AX\t4\n" +
                        "BZ\t4\n" +
                        "XX\t2\n" +
                        "AX\t5\n" +
                        "XX\t3\n" +
                        "AX\t6\n" +
                        "BZ\t5\n" +
                        "BZ\t6\n",
                "select str, dense_rank() rank over(partition by str) from 'abc'");
    }

    @Test
    public void testRankWithArg() throws Exception {
        try {
            expectFailure("select str, dense_rank(sym) rank over(partition by str) from 'abc'");
        } catch (ParserException e) {
            TestUtils.assertEquals("Unknown function", QueryError.getMessage());
        }
    }
}
