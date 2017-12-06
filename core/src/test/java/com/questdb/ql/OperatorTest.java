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

package com.questdb.ql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.Numbers;
import com.questdb.std.Rnd;
import com.questdb.std.time.Dates;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OperatorTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {

        // this does thread local allocations that
        // should not be accounted for while
        // measuring query allocations and de-allocations
        FACTORY_CONTAINER.getFactory().getConfiguration().exists("");

        try (JournalWriter w = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("abc")
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
    public void testDateEqualStr() throws Exception {
        assertThat("2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n",
                "select date, timestamp from abc where date = '2016-05-01T10:22:15.000Z'");
    }

    @Test
    public void testDateGreaterOrEqualDate() throws Exception {
        assertThat("2016-05-01T11:14:00.000Z\tXX\n" +
                        "2016-05-01T11:14:45.000Z\tXX\n" +
                        "2016-05-01T11:15:30.000Z\tKK\n" +
                        "2016-05-01T11:16:15.000Z\tXX\n" +
                        "2016-05-01T11:17:00.000Z\tKK\n",
                "select date, sym from abc where date >= '2016-05-01T11:14:00.000Z' limit 5");
    }

    @Test
    public void testDateGreaterOrEqualNull() throws Exception {
        assertEmpty("select date, sym from abc where date >= null");
    }

    @Test
    public void testDateGreaterOrEqualStr() throws Exception {
        assertThat("2016-05-01T11:14:00.000Z\tXX\n" +
                        "2016-05-01T11:14:45.000Z\tXX\n" +
                        "2016-05-01T11:15:30.000Z\tKK\n" +
                        "2016-05-01T11:16:15.000Z\tXX\n" +
                        "2016-05-01T11:17:00.000Z\tKK\n",
                "select date, sym from abc where date >= '2016-05-01T11:14:00.000Z' limit 5");
    }

    @Test
    public void testDateGreaterThan() throws Exception {
        assertThat("2016-05-01T10:21:00.000Z\t2016-05-01T10:20:45.000Z\n" +
                        "2016-05-01T10:22:00.000Z\t2016-05-01T10:21:30.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:22:15.000Z\n" +
                        "2016-05-01T10:24:00.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:25:00.000Z\t2016-05-01T10:23:45.000Z\n",
                "select timestamp, date from abc where timestamp > date limit 5");
    }

    @Test
    public void testDateGreaterThanNonConstStr() {
        try {
            expectFailure("select timestamp, date from abc where timestamp > str limit 5");
        } catch (ParserException e) {
            Assert.assertEquals(48, QueryError.getPosition());
        }
    }

    @Test
    public void testDateGreaterThanNull() throws Exception {
        assertEmpty("select timestamp, date from abc where date > null limit 5");
    }

    @Test
    public void testDateGreaterThanStr() throws Exception {
        assertThat("2016-05-01T10:25:00.000Z\t2016-05-01T10:23:45.000Z\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:24:30.000Z\n" +
                        "2016-05-01T10:27:00.000Z\t2016-05-01T10:25:15.000Z\n" +
                        "2016-05-01T10:28:00.000Z\t2016-05-01T10:26:00.000Z\n" +
                        "2016-05-01T10:29:00.000Z\t2016-05-01T10:26:45.000Z\n",
                "select timestamp, date from abc where date > '2016-05-01T10:23:00.000Z' limit 5");
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
    public void testDateInNulls() {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', null)");
        } catch (ParserException e) {
            Assert.assertEquals(78, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInTooFewArgs() {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z')");
        } catch (ParserException e) {
            Assert.assertEquals(46, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInTooManyArgs() {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', '2016-05-01T10:27:30.000Z','2016-05-01T10:27:30.000Z')");
        } catch (ParserException e) {
            Assert.assertEquals(105, QueryError.getPosition());
        }
    }

    @Test
    public void testDateInWrongArgs() {
        try {
            expectFailure("select date, timestamp, f from abc where date in ('2016-05-01T10:25:15.000Z', 10)");
        } catch (ParserException e) {
            Assert.assertEquals(78, QueryError.getPosition());
        }
    }

    @Test
    public void testDateLessOrEqualDate() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n",
                "select date, timestamp from abc where date <= timestamp limit 5");
    }

    @Test
    public void testDateLessOrEqualNull() throws Exception {
        assertEmpty("select date, timestamp from abc where date <= null");
    }

    @Test
    public void testDateLessOrEqualStr() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n" +
                        "2016-05-01T10:24:30.000Z\t2016-05-01T10:26:00.000Z\n",
                "select date, timestamp from abc where date <= '2016-05-01T10:24:30.000Z'");
    }

    @Test
    public void testDateLessThanDate() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n",
                "select date, timestamp from abc where date < timestamp limit 5");
    }

    @Test
    public void testDateLessThanNull() throws Exception {
        assertEmpty("select date, timestamp from abc where date < null");
    }

    @Test
    public void testDateLessThanStr() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n" +
                        "2016-05-01T10:24:30.000Z\t2016-05-01T10:26:00.000Z\n",
                "select date, timestamp from abc where date < '2016-05-01T10:25:00.000Z'");
    }

    @Test
    public void testDateNotEqualStr() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n" +
                        "2016-05-01T10:24:30.000Z\t2016-05-01T10:26:00.000Z\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:28:00.000Z\n" +
                        "2016-05-01T10:26:45.000Z\t2016-05-01T10:29:00.000Z\n" +
                        "2016-05-01T10:27:30.000Z\t2016-05-01T10:30:00.000Z\n" +
                        "2016-05-01T10:28:15.000Z\t2016-05-01T10:31:00.000Z\n",
                "select date, timestamp from abc where date != '2016-05-01T10:25:15.000Z' limit 10");
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
    public void testIntInNonConst() {
        try {
            expectFailure("select i, timestamp from abc where i in (1978144263, l) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(53, QueryError.getPosition());
        }
    }

    @Test
    public void testIntInWrongType() {
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
    public void testLongInWrongType() {
        try {
            expectFailure("select l, timestamp from abc where l in ('NaN', 9036423629723776443L, 3) limit 10");
        } catch (ParserException e) {
            Assert.assertEquals(41, QueryError.getPosition());
        }
    }

    @Test
    public void testNullGreaterOrEqualDate() throws Exception {
        assertEmpty("select date, sym from abc where null >= date");
    }

    @Test
    public void testNullGreaterThanDate() throws Exception {
        assertEmpty("select timestamp, date from abc where null > date");
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
    public void testShortGreater() throws Exception {
        assertThat("215.009567260742\t-32679\n" +
                        "-190.000000000000\t-7374\n",
                "select d, sho from abc where d > sho limit 2");

        assertThat("8000176386900538697\t27809\n" +
                        "7709707078566863064\t-7374\n",
                "select l, sho from abc where l > sho limit 2");

        assertThat("0.4836\t-32679\n" +
                        "0.6755\t-7374\n",
                "select f, sho from abc where f > sho limit 2");

        assertThat("1573662097\t21781\n" +
                        "1404198\t27809\n",
                "select i, sho from abc where i > sho limit 2");

        assertThat("21781\t27\n" +
                        "27809\t54\n",
                "select sho, b from abc where sho > b limit 2");

    }

    @Test
    public void testShortGreaterOrEqualDouble() throws Exception {
        assertThat("21781\t0.293202951550\n" +
                        "27809\t0.040750414133\n" +
                        "22298\t109.355468750000\n" +
                        "10633\t0.000002968513\n" +
                        "8010\t0.000156438433\n",
                "select sho, d from abc where sho >= d limit 5");
    }

    @Test
    public void testShortIn() throws Exception {
        assertThat("-7374\t2016-05-01T10:24:00.000Z\n" +
                        "-1605\t2016-05-01T10:30:00.000Z\n",
                "select sho, timestamp from abc where sho in (-7374,-1605)");
    }

    @Test
    public void testStrEqualsDate() throws Exception {
        assertThat("2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n",
                "select date, timestamp from abc where '2016-05-01T10:22:15.000Z' = date");
    }

    @Test
    public void testStrGreaterOrEqualDate() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\tKK\n" +
                        "2016-05-01T10:21:30.000Z\tXX\n" +
                        "2016-05-01T10:22:15.000Z\tAX\n" +
                        "2016-05-01T10:23:00.000Z\tBZ\n" +
                        "2016-05-01T10:23:45.000Z\tKK\n",
                "select date, sym from abc where '2016-05-01T10:23:45.000Z' >= date");
    }

    @Test
    public void testStrGreaterThanDate() throws Exception {
        assertThat("2016-05-01T10:21:00.000Z\t2016-05-01T10:20:45.000Z\n" +
                        "2016-05-01T10:22:00.000Z\t2016-05-01T10:21:30.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:22:15.000Z\n",
                "select timestamp, date from abc where '2016-05-01T10:23:00.000Z' > date");
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
    public void testStrInNonConst() {
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
    public void testStrInWrongType() {
        try {
            expectFailure("select str, timestamp from abc where str in (10) limit 20");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
        }
    }

    @Test
    public void testStrLessOrEqualDate() throws Exception {
        assertThat("2016-05-01T10:25:15.000Z\t2016-05-01T10:27:00.000Z\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:28:00.000Z\n" +
                        "2016-05-01T10:26:45.000Z\t2016-05-01T10:29:00.000Z\n" +
                        "2016-05-01T10:27:30.000Z\t2016-05-01T10:30:00.000Z\n" +
                        "2016-05-01T10:28:15.000Z\t2016-05-01T10:31:00.000Z\n",
                "select date, timestamp from abc where '2016-05-01T10:25:15.000Z' <= date limit 5");
    }

    @Test
    public void testStrLessThanDate() throws Exception {
        assertThat("2016-05-01T10:25:15.000Z\t2016-05-01T10:27:00.000Z\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:28:00.000Z\n" +
                        "2016-05-01T10:26:45.000Z\t2016-05-01T10:29:00.000Z\n" +
                        "2016-05-01T10:27:30.000Z\t2016-05-01T10:30:00.000Z\n" +
                        "2016-05-01T10:28:15.000Z\t2016-05-01T10:31:00.000Z\n",
                "select date, timestamp from abc where '2016-05-01T10:25:00.000Z' < date limit 5");
    }

    @Test
    public void testStrNotEqualDate() throws Exception {
        assertThat("2016-05-01T10:20:45.000Z\t2016-05-01T10:21:00.000Z\n" +
                        "2016-05-01T10:21:30.000Z\t2016-05-01T10:22:00.000Z\n" +
                        "2016-05-01T10:22:15.000Z\t2016-05-01T10:23:00.000Z\n" +
                        "2016-05-01T10:23:00.000Z\t2016-05-01T10:24:00.000Z\n" +
                        "2016-05-01T10:23:45.000Z\t2016-05-01T10:25:00.000Z\n" +
                        "2016-05-01T10:24:30.000Z\t2016-05-01T10:26:00.000Z\n" +
                        "2016-05-01T10:26:00.000Z\t2016-05-01T10:28:00.000Z\n" +
                        "2016-05-01T10:26:45.000Z\t2016-05-01T10:29:00.000Z\n" +
                        "2016-05-01T10:27:30.000Z\t2016-05-01T10:30:00.000Z\n" +
                        "2016-05-01T10:28:15.000Z\t2016-05-01T10:31:00.000Z\n",
                "select date, timestamp from abc where '2016-05-01T10:25:15.000Z' != date limit 10");
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

    @Test
    public void testVirtualColumnDateFilter() throws Exception {
        assertThat("2015-07-06T06:51:36.000Z\n" +
                        "2015-07-06T06:51:36.000Z\n",
                "(select toDate('1970-01-01T00:00:00.000Z') + 1436165496*1000L x from abc) where x = '2015-07-06T06:51:36.000Z' limit 2");
    }
}
