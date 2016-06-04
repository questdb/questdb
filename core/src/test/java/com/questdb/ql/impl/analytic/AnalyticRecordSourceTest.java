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

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.NoOpCancellationHandler;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import com.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class AnalyticRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("xyz")
                .$int("i")
                .$str("str")
                .$ts()
                .$())) {
            int n = 100;
            String[] sym = {"AX", "XX", "BZ", "KK"};
            Rnd rnd = new Rnd();

            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += 60000);
                ew.putInt(0, rnd.nextInt());
                ew.putStr(1, sym[rnd.nextPositiveInt() % sym.length]);
                ew.append();
            }
            w.commit();
        }
    }

    @Test
    @Ignore
    public void testCompilation() throws Exception {
        try {
            assertThat("", "select i, str, next(i) over (partition by str) from xyz");
        } catch (ParserException e) {
            System.out.println(QueryError.getMessage());
        }
    }

    @Test
    public void testSimple() throws Exception {
        final RecordSource recordSource = compiler.compileSource(factory, "xyz");
        sink.clear();

        final AnalyticRecordSource as = new AnalyticRecordSource(1024 * 1024, recordSource, new ObjList<AnalyticFunction>() {{
            add(new NextRowAnalyticFunction(16 * 1024, recordSource.getMetadata(), new ObjHashSet<String>() {{
                add("str");
            }}, "i"));
        }});

        sink.clear();
        RecordCursor cursor = as.prepareCursor(factory, NoOpCancellationHandler.INSTANCE);
        printer.printCursor(cursor);

        final String expected = "-1148479920\tBZ\t1970-01-01T00:00:00.000Z\t-409854405\n" +
                "1548800833\tKK\t1970-01-01T00:00:00.000Z\t73575701\n" +
                "73575701\tKK\t1970-01-01T00:00:00.000Z\t1326447242\n" +
                "1326447242\tKK\t1970-01-01T00:00:00.000Z\t-1436881714\n" +
                "1868723706\tAX\t1970-01-01T00:00:00.000Z\t-1191262516\n" +
                "-1191262516\tAX\t1970-01-01T00:00:00.000Z\t806715481\n" +
                "-1436881714\tKK\t1970-01-01T00:00:00.000Z\t1530831067\n" +
                "806715481\tAX\t1970-01-01T00:00:00.000Z\t1125579207\n" +
                "1569490116\tXX\t1970-01-01T00:00:00.000Z\t-1532328444\n" +
                "-409854405\tBZ\t1970-01-01T00:00:00.000Z\t1699553881\n" +
                "1530831067\tKK\t1970-01-01T00:00:00.000Z\t-1844391305\n" +
                "-1532328444\tXX\t1970-01-01T00:00:00.000Z\t1404198\n" +
                "1125579207\tAX\t1970-01-01T00:00:00.000Z\t-1432278050\n" +
                "-1432278050\tAX\t1970-01-01T00:00:00.000Z\t-85170055\n" +
                "-85170055\tAX\t1970-01-01T00:00:00.000Z\t-1125169127\n" +
                "-1844391305\tKK\t1970-01-01T00:00:00.000Z\t-1101822104\n" +
                "-1101822104\tKK\t1970-01-01T00:00:00.000Z\t-547127752\n" +
                "1404198\tXX\t1970-01-01T00:00:00.000Z\t1232884790\n" +
                "-1125169127\tAX\t1970-01-01T00:00:00.000Z\t-1975183723\n" +
                "-1975183723\tAX\t1970-01-01T00:00:00.000Z\t-2119387831\n" +
                "1232884790\tXX\t1970-01-01T00:00:00.000Z\t-1575135393\n" +
                "-2119387831\tAX\t1970-01-01T00:00:00.000Z\t1253890363\n" +
                "1699553881\tBZ\t1970-01-01T00:00:00.000Z\t-422941535\n" +
                "1253890363\tAX\t1970-01-01T00:00:00.000Z\t-2132716300\n" +
                "-422941535\tBZ\t1970-01-01T00:00:00.000Z\t-303295973\n" +
                "-547127752\tKK\t1970-01-01T00:00:00.000Z\t-461611463\n" +
                "-303295973\tBZ\t1970-01-01T00:00:00.000Z\t1890602616\n" +
                "-2132716300\tAX\t1970-01-01T00:00:00.000Z\t264240638\n" +
                "-461611463\tKK\t1970-01-01T00:00:00.000Z\t-2144581835\n" +
                "264240638\tAX\t1970-01-01T00:00:00.000Z\t-483853667\n" +
                "-483853667\tAX\t1970-01-01T00:00:00.000Z\t-2002373666\n" +
                "1890602616\tBZ\t1970-01-01T00:00:00.000Z\t68265578\n" +
                "68265578\tBZ\t1970-01-01T00:00:00.000Z\t458818940\n" +
                "-2002373666\tAX\t1970-01-01T00:00:00.000Z\t-1418341054\n" +
                "458818940\tBZ\t1970-01-01T00:00:00.000Z\t-2034804966\n" +
                "-2144581835\tKK\t1970-01-01T00:00:00.000Z\t2031014705\n" +
                "-1418341054\tAX\t1970-01-01T00:00:00.000Z\t-1787109293\n" +
                "2031014705\tKK\t1970-01-01T00:00:00.000Z\t936627841\n" +
                "-1575135393\tXX\t1970-01-01T00:00:00.000Z\t-372268574\n" +
                "936627841\tKK\t1970-01-01T00:00:00.000Z\t-667031149\n" +
                "-667031149\tKK\t1970-01-01T00:00:00.000Z\t1637847416\n" +
                "-2034804966\tBZ\t1970-01-01T00:00:00.000Z\t161592763\n" +
                "1637847416\tKK\t1970-01-01T00:00:00.000Z\t-1819240775\n" +
                "-1819240775\tKK\t1970-01-01T00:00:00.000Z\t-1201923128\n" +
                "-1787109293\tAX\t1970-01-01T00:00:00.000Z\t-1515787781\n" +
                "-1515787781\tAX\t1970-01-01T00:00:00.000Z\t636045524\n" +
                "161592763\tBZ\t1970-01-01T00:00:00.000Z\t-1299391311\n" +
                "636045524\tAX\t1970-01-01T00:00:00.000Z\t-1538602195\n" +
                "-1538602195\tAX\t1970-01-01T00:00:00.000Z\t-443320374\n" +
                "-372268574\tXX\t1970-01-01T00:00:00.000Z\t-10505757\n" +
                "-1299391311\tBZ\t1970-01-01T00:00:00.000Z\t1857212401\n" +
                "-10505757\tXX\t1970-01-01T00:00:00.000Z\t-1566901076\n" +
                "1857212401\tBZ\t1970-01-01T00:00:00.000Z\t1196016669\n" +
                "-443320374\tAX\t1970-01-01T00:00:00.000Z\t1234796102\n" +
                "1196016669\tBZ\t1970-01-01T00:00:00.000Z\t532665695\n" +
                "-1566901076\tXX\t1970-01-01T00:00:00.000Z\t1876812930\n" +
                "-1201923128\tKK\t1970-01-01T00:00:00.000Z\t-1582495445\n" +
                "1876812930\tXX\t1970-01-01T00:00:00.000Z\t-1172180184\n" +
                "-1582495445\tKK\t1970-01-01T00:00:00.000Z\t-45567293\n" +
                "532665695\tBZ\t1970-01-01T00:00:00.000Z\t-373499303\n" +
                "1234796102\tAX\t1970-01-01T00:00:00.000Z\t114747951\n" +
                "-45567293\tKK\t1970-01-01T00:00:00.000Z\t-916132123\n" +
                "-373499303\tBZ\t1970-01-01T00:00:00.000Z\t-1723887671\n" +
                "-916132123\tKK\t1970-01-01T00:00:00.000Z\t-731466113\n" +
                "114747951\tAX\t1970-01-01T00:00:00.000Z\t-1794809330\n" +
                "-1794809330\tAX\t1970-01-01T00:00:00.000Z\t-882371473\n" +
                "-731466113\tKK\t1970-01-01T00:00:00.000Z\t-2075675260\n" +
                "-882371473\tAX\t1970-01-01T00:00:00.000Z\t1235206821\n" +
                "-1723887671\tBZ\t1970-01-01T00:00:00.000Z\t-712702244\n" +
                "-1172180184\tXX\t1970-01-01T00:00:00.000Z\t865832060\n" +
                "-2075675260\tKK\t1970-01-01T00:00:00.000Z\t-1768335227\n" +
                "-712702244\tBZ\t1970-01-01T00:00:00.000Z\t1795359355\n" +
                "-1768335227\tKK\t1970-01-01T00:00:00.000Z\t-1966408995\n" +
                "1235206821\tAX\t1970-01-01T00:00:00.000Z\t838743782\n" +
                "1795359355\tBZ\t1970-01-01T00:00:00.000Z\t-876466531\n" +
                "-876466531\tBZ\t1970-01-01T00:00:00.000Z\t-2043803188\n" +
                "865832060\tXX\t1970-01-01T00:00:00.000Z\t614536941\n" +
                "-1966408995\tKK\t1970-01-01T00:00:00.000Z\t1107889075\n" +
                "838743782\tAX\t1970-01-01T00:00:00.000Z\t-618037497\n" +
                "1107889075\tKK\t1970-01-01T00:00:00.000Z\t-68027832\n" +
                "-618037497\tAX\t1970-01-01T00:00:00.000Z\t519895483\n" +
                "-2043803188\tBZ\t1970-01-01T00:00:00.000Z\t1658228795\n" +
                "-68027832\tKK\t1970-01-01T00:00:00.000Z\t-2088317486\n" +
                "519895483\tAX\t1970-01-01T00:00:00.000Z\t602835017\n" +
                "-2088317486\tKK\t1970-01-01T00:00:00.000Z\t-283321892\n" +
                "602835017\tAX\t1970-01-01T00:00:00.000Z\t-2111250190\n" +
                "-2111250190\tAX\t1970-01-01T00:00:00.000Z\t1598679468\n" +
                "614536941\tXX\t1970-01-01T00:00:00.000Z\t1015055928\n" +
                "1598679468\tAX\t1970-01-01T00:00:00.000Z\t1362833895\n" +
                "1658228795\tBZ\t1970-01-01T00:00:00.000Z\t1238491107\n" +
                "-283321892\tKK\t1970-01-01T00:00:00.000Z\t116799613\n" +
                "116799613\tKK\t1970-01-01T00:00:00.000Z\t-636975106\n" +
                "1238491107\tBZ\t1970-01-01T00:00:00.000Z\t1100812407\n" +
                "-636975106\tKK\t1970-01-01T00:00:00.000Z\t-640305320\n" +
                "1015055928\tXX\t1970-01-01T00:00:00.000Z\tNaN\n" +
                "1100812407\tBZ\t1970-01-01T00:00:00.000Z\t1751526583\n" +
                "1362833895\tAX\t1970-01-01T00:00:00.000Z\t-805434743\n" +
                "-805434743\tAX\t1970-01-01T00:00:00.000Z\tNaN\n" +
                "-640305320\tKK\t1970-01-01T00:00:00.000Z\tNaN\n" +
                "1751526583\tBZ\t1970-01-01T00:00:00.000Z\tNaN\n";
        TestUtils.assertEquals(expected, sink);
    }
}