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

package com.questdb.ql.impl;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Numbers;
import com.questdb.misc.Rnd;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.ql.parser.QueryError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InOperatorTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {

        // this does thread local allocations that
        // should not be accounted for while
        // measuring query allocations and de-allocations
        factory.getConfiguration().exists("");

        try (JournalWriter w = factory.bulkWriter(new JournalStructure("abc")
                .$int("i")
                .$double("d")
                .$float("f")
                .$byte("b")
                .$long("l")
                .$str("str")
                .$bool("boo")
                .$sym("sym")
                .$short("sho")
                .$date("date")
                .$ts()
                .$())) {
            int n = 1000;
            String[] sym = {"AX", "XX", "BZ", "KK"};
            Rnd rnd = new Rnd();

            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            long d = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += 60000);
                ew.putInt(0, (rnd.nextPositiveInt() & 15) == 0 ? Numbers.INT_NaN : rnd.nextInt());
                ew.putDouble(1, (rnd.nextPositiveInt() & 15) == 0 ? Double.NaN : rnd.nextDouble());
                ew.putFloat(2, (rnd.nextPositiveInt() & 15) == 0 ? Float.NaN : rnd.nextFloat());
                ew.put(3, (byte) rnd.nextInt());
                ew.putLong(4, (rnd.nextPositiveInt() & 15) == 0 ? Numbers.LONG_NaN : rnd.nextLong());
                ew.putStr(5, (rnd.nextPositiveInt() & 15) == 0 ? null : sym[rnd.nextPositiveInt() % sym.length]);
                ew.putBool(6, rnd.nextBoolean());
                ew.putSym(7, (rnd.nextPositiveInt() & 15) == 0 ? null : sym[rnd.nextPositiveInt() % sym.length]);
                ew.putShort(8, (short) rnd.nextInt());
                d += 45000;
                ew.putDate(9, (rnd.nextPositiveInt() & 15) == 0 ? Numbers.LONG_NaN : d);
                ew.append();
            }
            w.commit();
        }
    }

    @Test
    public void testByteIn() throws Exception {
        assertThat("-117\t2016-05-01T10:41:00.000Z\n" +
                        "-117\t2016-05-01T10:43:00.000Z\n" +
                        "-40\t2016-05-01T18:56:00.000Z\n" +
                        "-117\t2016-05-01T23:08:00.000Z\n",
                "select b, timestamp from abc where b in (-40, -117)");
    }

    @Test
    public void testDateIn() throws Exception {
        assertThat("2016-05-01T10:25:15.000Z\t2016-05-01T10:27:00.000Z\t0.3180\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:28:00.000Z\t0.2358\n" +
                        "2016-05-01T10:26:45.000Z\t2016-05-01T10:29:00.000Z\t0.1341\n" +
                        "2016-05-01T10:27:30.000Z\t2016-05-01T10:30:00.000Z\t0.6746\n",
                "select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', '2016-05-01T10:27:30.000Z')");
    }

    @Test
    public void testDateInNulls() throws Exception {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', null)");
        } catch (ParserException e) {
            Assert.assertEquals(78, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInTooFewArgs() throws Exception {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z')");
        } catch (ParserException e) {
            Assert.assertEquals(46, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInTooManyArgs() throws Exception {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', '2016-05-01T10:27:30.000Z','2016-05-01T10:27:30.000Z')");
        } catch (ParserException e) {
            Assert.assertEquals(105, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInWrongArgs() throws Exception {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', 10)");
        } catch (ParserException e) {
            Assert.assertEquals(78, QueryError.getPosition());
        }
    }

    @Test
    public void testInInt() throws Exception {
        assertThat("NaN\t2016-05-01T10:21:00.000Z\n" +
                        "724677640\t2016-05-01T10:47:00.000Z\n" +
                        "NaN\t2016-05-01T10:52:00.000Z\n" +
                        "NaN\t2016-05-01T10:55:00.000Z\n" +
                        "NaN\t2016-05-01T10:57:00.000Z\n" +
                        "NaN\t2016-05-01T11:07:00.000Z\n" +
                        "NaN\t2016-05-01T11:26:00.000Z\n" +
                        "NaN\t2016-05-01T11:33:00.000Z\n" +
                        "NaN\t2016-05-01T11:38:00.000Z\n" +
                        "NaN\t2016-05-01T11:54:00.000Z\n" +
                        "NaN\t2016-05-01T11:59:00.000Z\n" +
                        "NaN\t2016-05-01T12:27:00.000Z\n" +
                        "NaN\t2016-05-01T12:41:00.000Z\n" +
                        "NaN\t2016-05-01T12:59:00.000Z\n" +
                        "NaN\t2016-05-01T13:02:00.000Z\n" +
                        "NaN\t2016-05-01T13:07:00.000Z\n" +
                        "NaN\t2016-05-01T13:28:00.000Z\n" +
                        "NaN\t2016-05-01T13:50:00.000Z\n" +
                        "NaN\t2016-05-01T14:04:00.000Z\n" +
                        "NaN\t2016-05-01T15:04:00.000Z\n",
                "select i, timestamp from abc where i in (724677640, NaN) limit 20");
    }

    @Test
    public void testIntInNonConst() throws Exception {
        try {
            expectFailure("select i, timestamp from abc where i in (1978144263, l) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(53, QueryError.getPosition());
        }
    }

    @Test
    public void testIntInWrongType() throws Exception {
        try {
            expectFailure("select i, timestamp from abc where i in (1978144263L, NaN) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(41, QueryError.getPosition());
        }
    }

    @Test
    public void testLongIn() throws Exception {
        assertThat("-2653407051020864006\t2016-05-01T10:21:00.000Z\n" +
                        "NaN\t2016-05-01T10:53:00.000Z\n" +
                        "NaN\t2016-05-01T11:11:00.000Z\n" +
                        "NaN\t2016-05-01T11:31:00.000Z\n" +
                        "NaN\t2016-05-01T11:54:00.000Z\n" +
                        "NaN\t2016-05-01T12:10:00.000Z\n" +
                        "NaN\t2016-05-01T12:37:00.000Z\n" +
                        "NaN\t2016-05-01T12:52:00.000Z\n" +
                        "NaN\t2016-05-01T12:59:00.000Z\n" +
                        "NaN\t2016-05-01T13:13:00.000Z\n",
                "select l, timestamp from abc where l in (NaN, -2653407051020864006L) limit 10");
    }

    @Test
    public void testLongInMixArg() throws Exception {
        assertThat("8000176386900538697\t2016-05-01T10:23:00.000Z\n" +
                        "NaN\t2016-05-01T10:53:00.000Z\n" +
                        "NaN\t2016-05-01T11:11:00.000Z\n" +
                        "NaN\t2016-05-01T11:31:00.000Z\n" +
                        "NaN\t2016-05-01T11:54:00.000Z\n" +
                        "NaN\t2016-05-01T12:10:00.000Z\n" +
                        "NaN\t2016-05-01T12:37:00.000Z\n" +
                        "NaN\t2016-05-01T12:52:00.000Z\n" +
                        "NaN\t2016-05-01T12:59:00.000Z\n" +
                        "NaN\t2016-05-01T13:13:00.000Z\n",
                "select l, timestamp from abc where l in (NaN, 8000176386900538697L, 3) limit 10");
    }

    @Test
    public void testLongInWrongType() throws Exception {
        try {
            expectFailure("select l, timestamp from abc where l in ('NaN', 9036423629723776443L, 3) limit 10");
        } catch (ParserException e) {
            Assert.assertEquals(41, QueryError.getPosition());
        }
    }

    @Test
    public void testParserErrorOnNegativeNumbers() throws Exception {
        assertThat("NaN\t2016-05-01T10:21:00.000Z\n" +
                        "-1271909747\t2016-05-01T10:24:00.000Z\n" +
                        "NaN\t2016-05-01T10:52:00.000Z\n" +
                        "NaN\t2016-05-01T10:55:00.000Z\n" +
                        "NaN\t2016-05-01T10:57:00.000Z\n" +
                        "NaN\t2016-05-01T11:07:00.000Z\n" +
                        "NaN\t2016-05-01T11:26:00.000Z\n" +
                        "NaN\t2016-05-01T11:33:00.000Z\n" +
                        "NaN\t2016-05-01T11:38:00.000Z\n" +
                        "NaN\t2016-05-01T11:54:00.000Z\n",
                "select i, timestamp from abc where i  in (-1271909747, NaN) limit 10");
    }

    @Test
    public void testPrevWithNull() throws Exception {
        assertThat("XX\tAX\t\t2016-05-01T10:23:00.000Z\n" +
                        "XX\tKK\t\t2016-05-01T10:25:00.000Z\n" +
                        "XX\tBZ\t\t2016-05-01T10:27:00.000Z\n" +
                        "XX\tBZ\tXX\t2016-05-01T10:36:00.000Z\n" +
                        "XX\tBZ\tXX\t2016-05-01T10:39:00.000Z\n" +
                        "\tAX\tXX\t2016-05-01T10:41:00.000Z\n" +
                        "XX\t\t\t2016-05-01T10:42:00.000Z\n" +
                        "XX\tKK\tXX\t2016-05-01T10:43:00.000Z\n" +
                        "XX\tBZ\tXX\t2016-05-01T10:49:00.000Z\n" +
                        "XX\tAX\t\t2016-05-01T10:53:00.000Z\n" +
                        "XX\tKK\tXX\t2016-05-01T10:57:00.000Z\n" +
                        "XX\tKK\tXX\t2016-05-01T10:59:00.000Z\n" +
                        "XX\tXX\t\t2016-05-01T11:00:00.000Z\n" +
                        "\tKK\tXX\t2016-05-01T11:01:00.000Z\n" +
                        "XX\tAX\tXX\t2016-05-01T11:02:00.000Z\n" +
                        "XX\tAX\tXX\t2016-05-01T11:03:00.000Z\n" +
                        "XX\tKK\t\t2016-05-01T11:05:00.000Z\n" +
                        "XX\tBZ\tXX\t2016-05-01T11:08:00.000Z\n" +
                        "XX\tBZ\tXX\t2016-05-01T11:20:00.000Z\n" +
                        "XX\tAX\tXX\t2016-05-01T11:25:00.000Z\n",
                "select str, sym, prev(str) p over(partition by sym), timestamp from '*!*abc' where str in (null, 'XX') limit 20");
    }

    @Test
    public void testShortIn() throws Exception {
        assertThat("-7374\t2016-05-01T10:24:00.000Z\n" +
                        "-1605\t2016-05-01T10:30:00.000Z\n",
                "select sho, timestamp from abc where sho in (-7374,-1605)");
    }

    @Test
    public void testStrIn() throws Exception {
        assertThat("XX\t2016-05-01T10:23:00.000Z\n" +
                        "XX\t2016-05-01T10:25:00.000Z\n" +
                        "XX\t2016-05-01T10:27:00.000Z\n" +
                        "BZ\t2016-05-01T10:31:00.000Z\n" +
                        "BZ\t2016-05-01T10:33:00.000Z\n" +
                        "XX\t2016-05-01T10:36:00.000Z\n" +
                        "XX\t2016-05-01T10:39:00.000Z\n" +
                        "BZ\t2016-05-01T10:40:00.000Z\n" +
                        "XX\t2016-05-01T10:42:00.000Z\n" +
                        "XX\t2016-05-01T10:43:00.000Z\n",
                "select str, timestamp from abc where str in ('BZ', 'XX') limit 10");
    }

    @Test
    public void testStrInAsEq() throws Exception {
        assertThat("XX\t2016-05-01T10:23:00.000Z\n" +
                        "XX\t2016-05-01T10:25:00.000Z\n" +
                        "XX\t2016-05-01T10:27:00.000Z\n" +
                        "XX\t2016-05-01T10:36:00.000Z\n" +
                        "XX\t2016-05-01T10:39:00.000Z\n" +
                        "XX\t2016-05-01T10:42:00.000Z\n" +
                        "XX\t2016-05-01T10:43:00.000Z\n" +
                        "XX\t2016-05-01T10:49:00.000Z\n" +
                        "XX\t2016-05-01T10:53:00.000Z\n" +
                        "XX\t2016-05-01T10:57:00.000Z\n" +
                        "XX\t2016-05-01T10:59:00.000Z\n" +
                        "XX\t2016-05-01T11:00:00.000Z\n" +
                        "XX\t2016-05-01T11:02:00.000Z\n" +
                        "XX\t2016-05-01T11:03:00.000Z\n" +
                        "XX\t2016-05-01T11:05:00.000Z\n" +
                        "XX\t2016-05-01T11:08:00.000Z\n" +
                        "XX\t2016-05-01T11:20:00.000Z\n" +
                        "XX\t2016-05-01T11:25:00.000Z\n" +
                        "XX\t2016-05-01T11:26:00.000Z\n" +
                        "XX\t2016-05-01T11:34:00.000Z\n",
                "select str, timestamp from abc where str in ('XX') limit 20");
    }

    @Test
    public void testStrInNonConst() throws Exception {
        try {
            expectFailure("select str, timestamp from abc where str in ('X', sym) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(50, QueryError.getPosition());
        }
    }

    @Test
    public void testStrInNull() throws Exception {
        assertThat("XX\t2016-05-01T10:23:00.000Z\n" +
                        "XX\t2016-05-01T10:25:00.000Z\n" +
                        "XX\t2016-05-01T10:27:00.000Z\n" +
                        "XX\t2016-05-01T10:36:00.000Z\n" +
                        "XX\t2016-05-01T10:39:00.000Z\n" +
                        "\t2016-05-01T10:41:00.000Z\n" +
                        "XX\t2016-05-01T10:42:00.000Z\n" +
                        "XX\t2016-05-01T10:43:00.000Z\n" +
                        "XX\t2016-05-01T10:49:00.000Z\n" +
                        "XX\t2016-05-01T10:53:00.000Z\n" +
                        "XX\t2016-05-01T10:57:00.000Z\n" +
                        "XX\t2016-05-01T10:59:00.000Z\n" +
                        "XX\t2016-05-01T11:00:00.000Z\n" +
                        "\t2016-05-01T11:01:00.000Z\n" +
                        "XX\t2016-05-01T11:02:00.000Z\n" +
                        "XX\t2016-05-01T11:03:00.000Z\n" +
                        "XX\t2016-05-01T11:05:00.000Z\n" +
                        "XX\t2016-05-01T11:08:00.000Z\n" +
                        "XX\t2016-05-01T11:20:00.000Z\n" +
                        "XX\t2016-05-01T11:25:00.000Z\n",
                "select str, timestamp from abc where str in (null, 'XX') limit 20");
    }

    @Test
    public void testStrInWrongType() throws Exception {
        try {
            expectFailure("select str, timestamp from abc where str in (10) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
        }
    }

    @Test
    public void testSymIn() throws Exception {
        assertThat("KK\t2016-05-01T10:21:00.000Z\n" +
                        "XX\t2016-05-01T10:22:00.000Z\n" +
                        "KK\t2016-05-01T10:25:00.000Z\n" +
                        "KK\t2016-05-01T10:29:00.000Z\n" +
                        "KK\t2016-05-01T10:30:00.000Z\n" +
                        "XX\t2016-05-01T10:31:00.000Z\n" +
                        "KK\t2016-05-01T10:33:00.000Z\n" +
                        "KK\t2016-05-01T10:38:00.000Z\n" +
                        "KK\t2016-05-01T10:43:00.000Z\n" +
                        "XX\t2016-05-01T10:44:00.000Z\n" +
                        "KK\t2016-05-01T10:46:00.000Z\n" +
                        "XX\t2016-05-01T10:54:00.000Z\n" +
                        "KK\t2016-05-01T10:56:00.000Z\n" +
                        "KK\t2016-05-01T10:57:00.000Z\n" +
                        "XX\t2016-05-01T10:58:00.000Z\n" +
                        "KK\t2016-05-01T10:59:00.000Z\n" +
                        "XX\t2016-05-01T11:00:00.000Z\n" +
                        "KK\t2016-05-01T11:01:00.000Z\n" +
                        "KK\t2016-05-01T11:04:00.000Z\n" +
                        "KK\t2016-05-01T11:05:00.000Z\n",
                "select sym, timestamp from abc where sym in ('KK','XX') limit 20");
    }

    @Test
    public void testSymInAsEq() throws Exception {
        assertThat("KK\t2016-05-01T10:21:00.000Z\n" +
                        "KK\t2016-05-01T10:25:00.000Z\n" +
                        "KK\t2016-05-01T10:29:00.000Z\n" +
                        "KK\t2016-05-01T10:30:00.000Z\n" +
                        "KK\t2016-05-01T10:33:00.000Z\n" +
                        "KK\t2016-05-01T10:38:00.000Z\n" +
                        "KK\t2016-05-01T10:43:00.000Z\n" +
                        "KK\t2016-05-01T10:46:00.000Z\n" +
                        "KK\t2016-05-01T10:56:00.000Z\n" +
                        "KK\t2016-05-01T10:57:00.000Z\n" +
                        "KK\t2016-05-01T10:59:00.000Z\n" +
                        "KK\t2016-05-01T11:01:00.000Z\n" +
                        "KK\t2016-05-01T11:04:00.000Z\n" +
                        "KK\t2016-05-01T11:05:00.000Z\n" +
                        "KK\t2016-05-01T11:07:00.000Z\n" +
                        "KK\t2016-05-01T11:11:00.000Z\n" +
                        "KK\t2016-05-01T11:16:00.000Z\n" +
                        "KK\t2016-05-01T11:18:00.000Z\n" +
                        "KK\t2016-05-01T11:23:00.000Z\n" +
                        "KK\t2016-05-01T11:24:00.000Z\n",
                "select sym, timestamp from abc where sym in ('KK') limit 20");
    }

    @Test
    public void testSymInNull() throws Exception {
        assertThat("KK\t2016-05-01T10:21:00.000Z\n" +
                        "KK\t2016-05-01T10:25:00.000Z\n" +
                        "KK\t2016-05-01T10:29:00.000Z\n" +
                        "KK\t2016-05-01T10:30:00.000Z\n" +
                        "KK\t2016-05-01T10:33:00.000Z\n" +
                        "\t2016-05-01T10:35:00.000Z\n" +
                        "KK\t2016-05-01T10:38:00.000Z\n" +
                        "\t2016-05-01T10:42:00.000Z\n" +
                        "KK\t2016-05-01T10:43:00.000Z\n" +
                        "KK\t2016-05-01T10:46:00.000Z\n" +
                        "\t2016-05-01T10:55:00.000Z\n" +
                        "KK\t2016-05-01T10:56:00.000Z\n" +
                        "KK\t2016-05-01T10:57:00.000Z\n" +
                        "KK\t2016-05-01T10:59:00.000Z\n" +
                        "KK\t2016-05-01T11:01:00.000Z\n" +
                        "KK\t2016-05-01T11:04:00.000Z\n" +
                        "KK\t2016-05-01T11:05:00.000Z\n" +
                        "KK\t2016-05-01T11:07:00.000Z\n" +
                        "KK\t2016-05-01T11:11:00.000Z\n" +
                        "\t2016-05-01T11:14:00.000Z\n",
                "select sym, timestamp from abc where sym in (null, 'KK') limit 20");
    }
}
