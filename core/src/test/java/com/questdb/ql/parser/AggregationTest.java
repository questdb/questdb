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

package com.questdb.ql.parser;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.std.time.Dates;
import com.questdb.misc.Rnd;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AggregationTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int employeeCount = 10;
        try (JournalWriter orders = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("orders").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").
                        $ts("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        recordCountHint(recordCount).
                        $()
        )) {


            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                employees[i] = rnd.nextString(9);
            }

            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putInt(2, rnd.nextPositiveInt() % 200);
                w.putStr(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.append();
            }
            orders.commit();
        }

        try (JournalWriter stars = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("stars").
                        $int("galaxy").
                        $int("star").
                        $double("diameter").
                        $()
        )) {
            Rnd rnd = new Rnd();
            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = stars.entryWriter(timestamp += tsIncrement);
                w.putInt(0, rnd.nextPositiveInt() % 10);
                w.putInt(1, rnd.nextPositiveInt());
                int dividend = (rnd.nextPositiveInt() % 10);
                w.putDouble(2, Double.MAX_VALUE / (double) (dividend == 0 ? 1 : dividend));
                w.append();
            }
            stars.commit();
        }

        try (JournalWriter stars = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("stars2").
                        $int("galaxy").
                        $int("star").
                        $double("diameter").
                        $()
        )) {
            Rnd rnd = new Rnd();
            double r = Math.sqrt(Double.MAX_VALUE);
            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = stars.entryWriter(timestamp += tsIncrement);
                w.putInt(0, rnd.nextPositiveInt() % 10);
                w.putInt(1, rnd.nextPositiveInt());
                int dividend = (rnd.nextPositiveInt() % 10);
                w.putDouble(2, r / (double) (dividend == 0 ? 1 : dividend));
                w.append();
            }
            stars.commit();
        }
    }

    @Test
    public void testAggregateExpression() throws Exception {
        assertThat("employeeId\tsum\tsum2\tx\n" +
                        "TGPGWFFYU\t97328\t-21968.018329648252\t75359.981670351760\n" +
                        "DEYYQEHBH\t95288\t-4394.647402081921\t90893.352597918080\n" +
                        "SRYRFBVTM\t96798\t1945.437433247252\t98743.437433247248\n" +
                        "GZSXUXIBB\t97026\t3710.011166965701\t100736.011166965712\n" +
                        "UEDRQQULO\t104395\t-5341.399618807004\t99053.600381192992\n" +
                        "FOWLPDXYS\t98350\t-25051.961159685804\t73298.038840314192\n" +
                        "FJGETJRSZ\t103481\t-5023.046150211212\t98457.953849788784\n" +
                        "BEOUOJSHR\t96459\t-7031.317012047984\t89427.682987952016\n" +
                        "YRXPEHNRX\t96407\t-5897.650745672292\t90509.349254327712\n" +
                        "VTJWCPSWH\t102802\t-15878.443493302174\t86923.556506697824\n",
                "select employeeId, sum(productId) sum, sum(price) sum2, sum(price)+sum(productId) x from orders", true);
    }

    @Test
    public void testAvg() throws Exception {
        assertThat("employeeId\tcol0\tcol1\tcol2\n" +
                        "TGPGWFFYU\t-22.347933193945\t1.0571239707599186E9\t0.497876154503\n" +
                        "DEYYQEHBH\t-4.416731057369\t1.0578515420281407E9\t0.493395281078\n" +
                        "SRYRFBVTM\t2.007675369708\t1.0974368837843137E9\t0.506477851617\n" +
                        "GZSXUXIBB\t3.736164317186\t1.099431249958711E9\t0.504398549131\n" +
                        "UEDRQQULO\t-5.160772578557\t1.1102616773748791E9\t0.498863287248\n" +
                        "FOWLPDXYS\t-25.305011272410\t1.096727898191919E9\t0.502265356225\n" +
                        "FJGETJRSZ\t-4.886231663630\t1.0819423916371596E9\t0.493707956565\n" +
                        "BEOUOJSHR\t-7.182141993920\t1.0689834824116446E9\t0.500621256716\n" +
                        "YRXPEHNRX\t-5.903554299972\t1.0714308772062062E9\t0.507052500028\n" +
                        "VTJWCPSWH\t-15.430946057631\t1.0903374433984451E9\t0.493997276119\n",
                "select employeeId, avg(price), avg(quantity), avg(rate) from orders", true);
    }

    @Test
    public void testAvgConst() throws Exception {
        assertThat("employeeId\tcol0\n" +
                        "TGPGWFFYU\t100.300000000002\n" +
                        "DEYYQEHBH\t100.300000000002\n" +
                        "SRYRFBVTM\t100.300000000002\n" +
                        "GZSXUXIBB\t100.300000000002\n" +
                        "UEDRQQULO\t100.300000000002\n" +
                        "FOWLPDXYS\t100.300000000002\n" +
                        "FJGETJRSZ\t100.300000000002\n" +
                        "BEOUOJSHR\t100.300000000002\n" +
                        "YRXPEHNRX\t100.300000000002\n" +
                        "VTJWCPSWH\t100.300000000002\n",
                "select employeeId, avg(100.3) from orders", true);
    }

    @Test
    public void testAvgOverflow() throws Exception {
        assertThat("0\t6.873337142147506E307\n" +
                        "1\t6.873164995098181E307\n" +
                        "2\t6.942020245747746E307\n" +
                        "8\t7.080135168048353E307\n" +
                        "4\t7.139261077288051E307\n" +
                        "5\t6.98887936996273E307\n" +
                        "7\t6.903112119100632E307\n" +
                        "9\t6.731706356699814E307\n" +
                        "6\t7.051044382735928E307\n" +
                        "3\t7.088674983313755E307\n",
                "select galaxy, avg(diameter) d from stars");
    }

    @Test
    public void testCount() throws Exception {
        assertThat("TGPGWFFYU\t983\n" +
                        "DEYYQEHBH\t995\n" +
                        "SRYRFBVTM\t969\n" +
                        "GZSXUXIBB\t993\n" +
                        "UEDRQQULO\t1035\n" +
                        "FOWLPDXYS\t990\n" +
                        "FJGETJRSZ\t1028\n" +
                        "BEOUOJSHR\t979\n" +
                        "YRXPEHNRX\t999\n" +
                        "VTJWCPSWH\t1029\n",
                "select employeeId, count() from orders");
    }

    @Test
    public void testFirstDouble() throws Exception {
        assertThat("TGPGWFFYU\t172.796875000000\n" +
                        "DEYYQEHBH\t424.828125000000\n" +
                        "SRYRFBVTM\t153.473033905029\n" +
                        "GZSXUXIBB\t632.921875000000\n" +
                        "UEDRQQULO\t0.000000009901\n" +
                        "FOWLPDXYS\t0.003103211522\n" +
                        "FJGETJRSZ\t1.229880273342\n" +
                        "BEOUOJSHR\t364.462486267090\n" +
                        "YRXPEHNRX\t0.000000261681\n" +
                        "VTJWCPSWH\t-144.421875000000\n",
                "select employeeId, first(price) f from orders");
    }

    @Test
    public void testFirstFloat() throws Exception {
        assertThat("TGPGWFFYU\t0.5832\n" +
                        "DEYYQEHBH\t0.2858\n" +
                        "SRYRFBVTM\t0.3455\n" +
                        "GZSXUXIBB\t0.5619\n" +
                        "UEDRQQULO\t0.1498\n" +
                        "FOWLPDXYS\t0.2931\n" +
                        "FJGETJRSZ\t0.7276\n" +
                        "BEOUOJSHR\t0.2870\n" +
                        "YRXPEHNRX\t0.8434\n" +
                        "VTJWCPSWH\t0.5373\n",
                "select employeeId, first(rate) f from orders");
    }

    @Test
    public void testFirstInt() throws Exception {
        assertThat("TGPGWFFYU\t1920890138\n" +
                        "DEYYQEHBH\t98924388\n" +
                        "SRYRFBVTM\t1876812930\n" +
                        "GZSXUXIBB\t572338288\n" +
                        "UEDRQQULO\t712702244\n" +
                        "FOWLPDXYS\t2060263242\n" +
                        "FJGETJRSZ\t544695670\n" +
                        "BEOUOJSHR\t923501161\n" +
                        "YRXPEHNRX\t230430837\n" +
                        "VTJWCPSWH\t1960168360\n",
                "select employeeId, first(quantity) f from orders");
    }

    @Test
    public void testFirstLong() throws Exception {
        assertThat("TGPGWFFYU\t2014-05-04T10:30:10.000Z\n" +
                        "DEYYQEHBH\t2014-05-04T10:30:30.000Z\n" +
                        "SRYRFBVTM\t2014-05-04T10:30:40.000Z\n" +
                        "GZSXUXIBB\t2014-05-04T10:30:50.000Z\n" +
                        "UEDRQQULO\t2014-05-04T10:31:20.000Z\n" +
                        "FOWLPDXYS\t2014-05-04T10:31:30.000Z\n" +
                        "FJGETJRSZ\t2014-05-04T10:31:50.000Z\n" +
                        "BEOUOJSHR\t2014-05-04T10:32:50.000Z\n" +
                        "YRXPEHNRX\t2014-05-04T10:33:10.000Z\n" +
                        "VTJWCPSWH\t2014-05-04T10:34:10.000Z\n",
                "select employeeId, ltod(first(orderDate)) f from orders");
    }

    @Test
    public void testLSumInt() throws Exception {
        assertThat("TGPGWFFYU\t1039152863257\t-229222375\n" +
                        "DEYYQEHBH\t1052562284318\t295296798\n" +
                        "SRYRFBVTM\t1063416340387\t-1735549021\n" +
                        "GZSXUXIBB\t1091735231209\t813538025\n" +
                        "UEDRQQULO\t1149120836083\t-1930399245\n" +
                        "FOWLPDXYS\t1085760619210\t-866106678\n" +
                        "FJGETJRSZ\t1112236778603\t-159751061\n" +
                        "BEOUOJSHR\t1046534829281\t-1437190943\n" +
                        "YRXPEHNRX\t1070359446329\t912589625\n" +
                        "VTJWCPSWH\t1121957229257\t970765001\n",
                "select employeeId, lsum(quantity) s, sum(quantity) s2 from orders");

    }

    @Test
    public void testLastDouble() throws Exception {
        assertThat("employeeId\tcol0\tcol1\tcol2\tcol3\tcol5\tcol6\n" +
                        "TGPGWFFYU\t0.005398272420\t0.4752\t1801096068\t2014-05-05T14:14:10.000Z\t2014-05-04T10:30:10.000Z\t2014-05-05T14:14:10.000Z\n" +
                        "DEYYQEHBH\t858.651367187500\t0.6052\t253116346\t2014-05-05T14:16:20.000Z\t2014-05-04T10:30:30.000Z\t2014-05-05T14:16:20.000Z\n" +
                        "SRYRFBVTM\t21.549713134766\t0.4888\t1518306371\t2014-05-05T14:14:50.000Z\t2014-05-04T10:30:40.000Z\t2014-05-05T14:14:50.000Z\n" +
                        "GZSXUXIBB\t328.000000000000\t0.5024\t1896175587\t2014-05-05T14:16:30.000Z\t2014-05-04T10:30:50.000Z\t2014-05-05T14:16:30.000Z\n" +
                        "UEDRQQULO\t-651.000000000000\t0.4547\t260995870\t2014-05-05T14:16:40.000Z\t2014-05-04T10:31:20.000Z\t2014-05-05T14:16:40.000Z\n" +
                        "FOWLPDXYS\t-727.085937500000\t0.4486\t2005631\t2014-05-05T14:16:10.000Z\t2014-05-04T10:31:30.000Z\t2014-05-05T14:16:10.000Z\n" +
                        "FJGETJRSZ\t116.035564422607\t0.6497\t987587702\t2014-05-05T14:13:20.000Z\t2014-05-04T10:31:50.000Z\t2014-05-05T14:13:20.000Z\n" +
                        "BEOUOJSHR\t-233.000000000000\t0.2665\t1504681377\t2014-05-05T14:14:30.000Z\t2014-05-04T10:32:50.000Z\t2014-05-05T14:14:30.000Z\n" +
                        "YRXPEHNRX\t0.000003891365\t0.6637\t1081845029\t2014-05-05T14:12:10.000Z\t2014-05-04T10:33:10.000Z\t2014-05-05T14:12:10.000Z\n" +
                        "VTJWCPSWH\t124.287727355957\t0.5628\t414901203\t2014-05-05T14:15:10.000Z\t2014-05-04T10:34:10.000Z\t2014-05-05T14:15:10.000Z\n",
                "select employeeId, last(price), last(rate), last(quantity), ltod(dtol(last(orderDate))), min(orderDate), max(orderDate) from orders",
                true);

    }

    @Test
    public void testResampling() throws Exception {
        assertThat("2014-05-04T00:00:00.000Z\tTGPGWFFYU\t-63.253453401381\t-63.253453401381\n" +
                        "2014-05-04T00:00:00.000Z\tDEYYQEHBH\t17.232482911526\t17.232482911526\n" +
                        "2014-05-04T00:00:00.000Z\tSRYRFBVTM\t-10.581027815832\t-10.581027815832\n" +
                        "2014-05-04T00:00:00.000Z\tGZSXUXIBB\t1.191841183028\t1.191841183028\n" +
                        "2014-05-04T00:00:00.000Z\tUEDRQQULO\t-25.284387331977\t-25.284387331977\n" +
                        "2014-05-04T00:00:00.000Z\tFOWLPDXYS\t-21.110275361914\t-21.110275361914\n" +
                        "2014-05-04T00:00:00.000Z\tFJGETJRSZ\t-12.327370360108\t-12.327370360108\n" +
                        "2014-05-04T00:00:00.000Z\tBEOUOJSHR\t3.586645530510\t3.586645530510\n" +
                        "2014-05-04T00:00:00.000Z\tYRXPEHNRX\t-10.131327938006\t-10.131327938006\n" +
                        "2014-05-04T00:00:00.000Z\tVTJWCPSWH\t-24.329569665466\t-24.329569665466\n" +
                        "2014-05-05T00:00:00.000Z\tDEYYQEHBH\t-30.963486961448\t-30.963486961448\n" +
                        "2014-05-05T00:00:00.000Z\tSRYRFBVTM\t13.422138958032\t13.422138958032\n" +
                        "2014-05-05T00:00:00.000Z\tVTJWCPSWH\t0.595780540587\t0.595780540587\n" +
                        "2014-05-05T00:00:00.000Z\tBEOUOJSHR\t-31.682205368795\t-31.682205368795\n" +
                        "2014-05-05T00:00:00.000Z\tFJGETJRSZ\t-31.906856870748\t-31.906856870748\n" +
                        "2014-05-05T00:00:00.000Z\tGZSXUXIBB\t7.870801180456\t7.870801180456\n" +
                        "2014-05-05T00:00:00.000Z\tFOWLPDXYS\t-11.860556414848\t-11.860556414848\n" +
                        "2014-05-05T00:00:00.000Z\tYRXPEHNRX\t-8.573401980346\t-8.573401980346\n" +
                        "2014-05-05T00:00:00.000Z\tUEDRQQULO\t16.987375521363\t16.987375521363\n" +
                        "2014-05-05T00:00:00.000Z\tTGPGWFFYU\t17.260132823173\t17.260132823173\n",
                "select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders sample by 1d");
    }

    @Test
    public void testResampling2() throws Exception {
        assertThat("2014-05-04T08:00:00.000Z\t-18.041874103485\n" +
                        "2014-05-04T16:00:00.000Z\t-12.148285354848\n" +
                        "2014-05-05T00:00:00.000Z\t-10.773253435499\n" +
                        "2014-05-05T08:00:00.000Z\t0.750778769143\n",
                "select orderDate, vwap(price, quantity) from orders sample by 8h");
    }

    @Test
    public void testResamplingAliasClash() throws Exception {
        try {
            expectFailure("select dtoa4(orderDate) orderDate, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders sample by 1d");
        } catch (ParserException e) {
            Assert.assertEquals(24, QueryError.getPosition());
        }
    }

    @Test
    public void testResamplingNoAggregates() throws Exception {
        try {
            expectFailure("select orderDate, price+quantity from orders sample by 8h");
        } catch (ParserException e) {
            Assert.assertEquals(55, QueryError.getPosition());
        }
    }

    @Test
    public void testResamplingTimestampRename() throws Exception {
        assertThat("2014-05-04T08:00:00.000Z\t-18.041874103485\n" +
                        "2014-05-04T16:00:00.000Z\t-12.148285354848\n" +
                        "2014-05-05T00:00:00.000Z\t-10.773253435499\n" +
                        "2014-05-05T08:00:00.000Z\t0.750778769143\n",
                "select orderDate ts, vwap(price, quantity) from orders sample by 8h");
    }

    @Test
    public void testRowIdCompliance() throws Exception {
        assertRowId("select employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders", "employeeId");
    }

    @Test
    public void testStdDevOverflow() throws Exception {
        assertThat("0\t1.9559640924336714E307\t4.4226282824059176E153\n" +
                        "1\t1.879534937588584E307\t4.335360351330191E153\n" +
                        "2\t1.9686634860114622E307\t4.436962346032995E153\n" +
                        "8\t2.0009174148155284E307\t4.473161538347937E153\n" +
                        "4\t2.0331647100026274E307\t4.5090627740170434E153\n" +
                        "5\t1.951822822449265E307\t4.417943891053015E153\n" +
                        "7\t1.931400940102605E307\t4.3947706881049034E153\n" +
                        "9\t1.841320042762572E307\t4.291060524815016E153\n" +
                        "6\t1.9512460199745866E307\t4.4172910476609834E153\n" +
                        "3\t1.9942851937157214E307\t4.4657420365665115E153\n",
                "select galaxy, var(diameter), stddev(diameter) d from stars2");
    }

    @Test
    public void testSumConst() throws Exception {
        assertThat("employeeId\tsum\tcol0\tcol1\n" +
                        "TGPGWFFYU\t983\t10\t20\n" +
                        "DEYYQEHBH\t995\t10\t20\n" +
                        "SRYRFBVTM\t969\t10\t20\n" +
                        "GZSXUXIBB\t993\t10\t20\n" +
                        "UEDRQQULO\t1035\t10\t20\n" +
                        "FOWLPDXYS\t990\t10\t20\n" +
                        "FJGETJRSZ\t1028\t10\t20\n" +
                        "BEOUOJSHR\t979\t10\t20\n" +
                        "YRXPEHNRX\t999\t10\t20\n" +
                        "VTJWCPSWH\t1029\t10\t20\n",
                "select employeeId, sum(1) sum, min(10), max(20) from orders", true);
    }

    @Test
    public void testSumDouble() throws Exception {
        assertThat("employeeId\tsum\tcol0\tcol1\n" +
                        "TGPGWFFYU\t-21968.018329648252\t-1024.000000000000\t1017.000000000000\n" +
                        "DEYYQEHBH\t-4394.647402081921\t-1024.000000000000\t1014.750000000000\n" +
                        "SRYRFBVTM\t1945.437433247252\t-1024.000000000000\t1014.000000000000\n" +
                        "GZSXUXIBB\t3710.011166965701\t-1024.000000000000\t1000.750000000000\n" +
                        "UEDRQQULO\t-5341.399618807004\t-1024.000000000000\t1023.335937500000\n" +
                        "FOWLPDXYS\t-25051.961159685804\t-1024.000000000000\t1022.250000000000\n" +
                        "FJGETJRSZ\t-5023.046150211212\t-1024.000000000000\t1016.937500000000\n" +
                        "BEOUOJSHR\t-7031.317012047984\t-1024.000000000000\t1016.375000000000\n" +
                        "YRXPEHNRX\t-5897.650745672292\t-1024.000000000000\t1020.442382812500\n" +
                        "VTJWCPSWH\t-15878.443493302174\t-1024.000000000000\t1016.000000000000\n",
                "select employeeId, sum(price) sum, min(price), max(price) from orders", true);
    }

    @Test
    public void testSumFloat() throws Exception {
        assertThat("TGPGWFFYU\t489.412259876728\t0.017976403236\t0.994569778442\n" +
                        "DEYYQEHBH\t490.928304672241\t0.002583622932\t0.990825176239\n" +
                        "SRYRFBVTM\t490.777038216591\t0.015948891640\t0.996204674244\n" +
                        "GZSXUXIBB\t500.867759287357\t0.023229062557\t0.984000325203\n" +
                        "UEDRQQULO\t516.323502302170\t0.005706131458\t0.984908044338\n" +
                        "FOWLPDXYS\t497.242702662945\t0.020444750786\t0.967337131500\n" +
                        "FJGETJRSZ\t507.531779348850\t0.006864905357\t0.970933258533\n" +
                        "BEOUOJSHR\t490.108210325241\t0.014558494091\t0.981236577034\n" +
                        "YRXPEHNRX\t506.545447528362\t0.022396981716\t0.979304194450\n" +
                        "VTJWCPSWH\t508.323197126389\t0.015372276306\t0.997526228428\n",
                "select employeeId, sum(rate), min(rate), max(rate) s from orders");

    }

    @Test
    public void testSumInt() throws Exception {
        assertThat("employeeId\tsum\tcol0\tcol1\n" +
                        "TGPGWFFYU\t97328\t0\t199\n" +
                        "DEYYQEHBH\t95288\t0\t199\n" +
                        "SRYRFBVTM\t96798\t0\t199\n" +
                        "GZSXUXIBB\t97026\t0\t199\n" +
                        "UEDRQQULO\t104395\t0\t199\n" +
                        "FOWLPDXYS\t98350\t0\t199\n" +
                        "FJGETJRSZ\t103481\t0\t199\n" +
                        "BEOUOJSHR\t96459\t0\t199\n" +
                        "YRXPEHNRX\t96407\t0\t199\n" +
                        "VTJWCPSWH\t102802\t0\t199\n",
                "select employeeId, sum(productId) sum, min(productId), max(productId) from orders", true);
    }

    @Test
    public void testVWapConst() throws Exception {
        assertThat("TGPGWFFYU\t-21.643293565756\t10.000000000000\n" +
                        "DEYYQEHBH\t-6.467001028408\t10.000000000000\n" +
                        "SRYRFBVTM\t2.393438946531\t10.000000000000\n" +
                        "GZSXUXIBB\t4.741280909223\t10.000000000000\n" +
                        "UEDRQQULO\t-3.726755343047\t10.000000000000\n" +
                        "FOWLPDXYS\t-16.216304999514\t10.000000000000\n" +
                        "FJGETJRSZ\t-22.689574330892\t10.000000000000\n" +
                        "BEOUOJSHR\t-15.105882600563\t10.000000000000\n" +
                        "YRXPEHNRX\t-9.386559884214\t10.000000000000\n" +
                        "VTJWCPSWH\t-12.402215320133\t10.000000000000\n",
                "select employeeId, sum(price*quantity)/lsum(quantity), vwap(10, 20) sum from orders");
    }

    @Test
    public void testVWapDoubleDouble() throws Exception {
        assertThat("TGPGWFFYU\t-21.643293565756\t-21.643293565756\n" +
                        "DEYYQEHBH\t-6.467001028408\t-6.467001028408\n" +
                        "SRYRFBVTM\t2.393438946531\t2.393438946531\n" +
                        "GZSXUXIBB\t4.741280909223\t4.741280909223\n" +
                        "UEDRQQULO\t-3.726755343047\t-3.726755343047\n" +
                        "FOWLPDXYS\t-16.216304999514\t-16.216304999514\n" +
                        "FJGETJRSZ\t-22.689574330892\t-22.689574330892\n" +
                        "BEOUOJSHR\t-15.105882600563\t-15.105882600563\n" +
                        "YRXPEHNRX\t-9.386559884214\t-9.386559884214\n" +
                        "VTJWCPSWH\t-12.402215320133\t-12.402215320133\n",
                "select employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders");
    }

    @Test
    public void testVarianceOverflow() throws Exception {
        assertThat("0\tNaN\n" +
                        "1\tNaN\n" +
                        "2\tNaN\n" +
                        "8\tNaN\n" +
                        "4\tNaN\n" +
                        "5\tNaN\n" +
                        "7\tNaN\n" +
                        "9\tNaN\n" +
                        "6\tNaN\n" +
                        "3\tNaN\n",
                "select galaxy, var(diameter) d from stars");
    }

    @Test
    public void testVarianceOverflow2() throws Exception {
        assertThat("0\t1.9559640924336714E307\n" +
                        "1\t1.879534937588584E307\n" +
                        "2\t1.9686634860114622E307\n" +
                        "8\t2.0009174148155284E307\n" +
                        "4\t2.0331647100026274E307\n" +
                        "5\t1.951822822449265E307\n" +
                        "7\t1.931400940102605E307\n" +
                        "9\t1.841320042762572E307\n" +
                        "6\t1.9512460199745866E307\n" +
                        "3\t1.9942851937157214E307\n",
                "select galaxy, var(diameter) d from stars2");
    }
}
