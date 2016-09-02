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
import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import com.questdb.ql.parser.AbstractOptimiserTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class JournalRecordSourceTest extends AbstractOptimiserTest {
    @BeforeClass
    public static void setUp() throws Exception {
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("parent")
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
                .partitionBy(PartitionBy.DAY)
                .$())) {


            Rnd rnd = new Rnd();
            int n = 24 * 3; // number of records
            int timestep = 60 * 60 * 1000; // time stepping 1hr
            String[] sym = {"AX", "XX", "BZ", "KK", "PP", "UX", "LK"};
            String[] str = new String[16];
            for (int i = 0; i < str.length; i++) {
                str[i] = rnd.nextString(rnd.nextPositiveInt() % 10);
            }

            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += timestep);
                ew.putInt(0, rnd.nextInt() % 15);
                ew.putDouble(1, rnd.nextDouble());
                ew.putFloat(2, rnd.nextFloat());
                ew.put(3, (byte) rnd.nextInt());
                ew.putLong(4, rnd.nextLong() % 30);
                ew.putStr(5, str[rnd.nextPositiveInt() % str.length]);
                ew.putBool(6, rnd.nextBoolean());
                ew.putSym(7, sym[rnd.nextPositiveInt() % sym.length]);
                ew.putShort(8, (short) (rnd.nextInt() % 20));
                ew.putDate(9, rnd.nextLong());
                ew.append();
            }
            w.commit();
        }
    }

    @Test
    public void testFilterRowId() throws Exception {
        assertRowId("parent where sym = 'KK'", "date");
    }

    @Test
    public void testIntervalRowId() throws Exception {
        assertRowId("parent where timestamp = '2016-05-01T23;1h'", "date");
    }

    @Test
    public void testOrderRowId() throws Exception {
        assertRowId("parent order by date", "timestamp");
    }

    @Test
    public void testSelect() throws Exception {
        final String expected = "5\t0.047392679378\t0.3350\t-127\t3\tHRUEDRQ\ttrue\tUX\t18\t000-279210597-01-0-2093597T14:28:07.160Z\t2016-05-01T11:20:00.000Z\n" +
                "-14\t-284.232421875000\t0.4112\t-48\t17\t\tfalse\tLK\t4\t258932632-12-1941365T22:24:08.944Z\t2016-05-01T12:20:00.000Z\n" +
                "-10\t-512.000000000000\t0.7163\t-79\t12\tHRUEDRQ\ttrue\tBZ\t-2\t185885954-12-1393486T18:31:09.514Z\t2016-05-01T13:20:00.000Z\n" +
                "-3\t424.828125000000\t0.2858\t55\t-2\tGZSXUXI\ttrue\tAX\t-5\t201959183-01-1514667T09:59:49.837Z\t2016-05-01T14:20:00.000Z\n" +
                "5\t0.000000009480\t0.8258\t-61\t23\tEYYQEHB\ttrue\tPP\t-3\t258449442-12-1937712T09:05:26.575Z\t2016-05-01T15:20:00.000Z\n" +
                "8\t557.000000000000\t0.5172\t-41\t-21\tF\tfalse\tAX\t-4\t47691466-12-357202T00:16:53.191Z\t2016-05-01T16:20:00.000Z\n" +
                "-10\t0.060836097226\t0.6644\t-123\t1\t\ttrue\tLK\t2\t29025300-12-217219T23:29:37.949Z\t2016-05-01T17:20:00.000Z\n" +
                "-3\t300.163238525391\t0.5051\t-109\t-22\t\tfalse\tUX\t-17\t000-172367183-01-0-1292350T13:59:33.528Z\t2016-05-01T18:20:00.000Z\n" +
                "-13\t0.107344331220\t0.4957\t-69\t-22\t\tfalse\tXX\t4\t000-282358006-12-0-2117589T22:07:48.466Z\t2016-05-01T19:20:00.000Z\n" +
                "12\t0.248532287776\t0.4957\t-88\t23\tXY\tfalse\tPP\t13\t000-259637437-01-0-1946659T05:28:26.667Z\t2016-05-01T20:20:00.000Z\n" +
                "12\t-583.609375000000\t0.6253\t56\t-15\tEYYQEHB\ttrue\tKK\t0\t000-156777337-01-0-1175276T20:51:19.305Z\t2016-05-01T21:20:00.000Z\n" +
                "8\t-864.392822265625\t0.7237\t-62\t19\tEYYQEHB\tfalse\tPP\t-6\t147073946-01-1102991T02:54:29.993Z\t2016-05-01T22:20:00.000Z\n" +
                "0\t-119.000000000000\t0.3158\t-55\t-23\tHRUEDRQ\tfalse\tXX\t14\t253793133-12-1903076T01:48:28.243Z\t2016-05-01T23:20:00.000Z\n" +
                "-2\t-352.000000000000\t0.5488\t29\t-9\t\tfalse\tPP\t5\t000-285010246-12-0-2137399T04:17:15.224Z\t2016-05-02T00:20:00.000Z\n" +
                "9\t134.663085937500\t0.5315\t-91\t-16\tGZSXUXI\tfalse\tPP\t-3\t245888707-12-1843634T03:30:38.709Z\t2016-05-02T01:20:00.000Z\n" +
                "3\t0.010311927646\t0.4648\t68\t17\tGPGWFFYU\ttrue\tKK\t-16\t000-226684804-01-0-1699695T21:25:17.104Z\t2016-05-02T02:20:00.000Z\n" +
                "-11\t0.000000032913\t0.3010\t98\t-4\t\ttrue\tBZ\t-9\t75523745-12-566041T22:11:27.250Z\t2016-05-02T03:20:00.000Z\n" +
                "-10\t-144.421875000000\t0.5373\t46\t8\tGPGWFFYU\tfalse\tXX\t0\t000-256533984-12-0-1923736T13:36:52.642Z\t2016-05-02T04:20:00.000Z\n" +
                "7\t0.000000064758\t0.4588\t44\t14\tUOJ\tfalse\tBZ\t-12\t223661112-01-1677277T09:36:06.226Z\t2016-05-02T05:20:00.000Z\n" +
                "-11\t-911.000000000000\t0.3472\t39\t-15\tLOFJ\ttrue\tLK\t-10\t000-260007002-01-0-1949471T16:21:52.892Z\t2016-05-02T06:20:00.000Z\n" +
                "13\t-589.718750000000\t0.0314\t12\t26\tXPEHNR\ttrue\tBZ\t9\t219837603-12-1648327T17:57:00.303Z\t2016-05-02T07:20:00.000Z\n" +
                "-11\t1.167305707932\t0.5309\t-128\t3\t\tfalse\tAX\t6\t000-141022171-12-0-1057615T08:09:18.874Z\t2016-05-02T08:20:00.000Z\n" +
                "-11\t15.713242053986\t0.5207\t58\t10\tUOJ\tfalse\tAX\t19\t63570933-12-476421T11:00:13.287Z\t2016-05-02T09:20:00.000Z\n" +
                "-12\t296.611343383789\t0.2628\t-48\t17\tGPGWFFYU\tfalse\tAX\t14\t000-216109110-12-0-1620693T23:21:50.616Z\t2016-05-02T10:20:00.000Z\n" +
                "-14\t0.000000248992\t0.5233\t92\t28\t\tfalse\tKK\t-16\t121070486-01-907786T04:40:57.855Z\t2016-05-02T11:20:00.000Z\n" +
                "-6\t0.000014454320\t0.5999\t-77\t-6\tHRUEDRQ\tfalse\tPP\t8\t214004711-01-1604960T03:32:20.665Z\t2016-05-02T12:20:00.000Z\n" +
                "-14\t0.000000119284\t0.2506\t-12\t-15\tLOFJ\tfalse\tPP\t-12\t119513416-01-896203T20:43:31.509Z\t2016-05-02T13:20:00.000Z\n" +
                "-14\t7.793050527573\t0.3849\t-109\t-15\tWLP\ttrue\tUX\t-7\t000-234455102-12-0-1758214T17:49:54.678Z\t2016-05-02T14:20:00.000Z\n" +
                "-3\t0.001914593857\t0.2802\t46\t-6\tGPGWFFYU\tfalse\tLK\t-17\t000-271106876-12-0-2033118T20:33:53.536Z\t2016-05-02T15:20:00.000Z\n" +
                "-12\t0.000000001910\t0.3988\t-79\t-8\tWLP\ttrue\tPP\t15\t183927937-12-1379061T07:01:08.360Z\t2016-05-02T16:20:00.000Z\n" +
                "6\t0.000000000000\t0.3820\t-49\t5\tGZSXUXI\tfalse\tBZ\t-18\t000-197156282-01-0-1478033T04:43:10.644Z\t2016-05-02T17:20:00.000Z\n" +
                "4\t0.000000000000\t0.4107\t-5\t19\t\tfalse\tLK\t2\t262462901-01-1968302T09:15:31.151Z\t2016-05-02T18:20:00.000Z\n" +
                "-11\t0.000001089423\t0.2162\t-43\t15\tLOFJ\tfalse\tPP\t0\t209084855-01-1567973T04:29:10.493Z\t2016-05-02T19:20:00.000Z\n" +
                "0\t0.000289257550\t0.4044\t42\t0\tHRUEDRQ\tfalse\tAX\t10\t000-161822811-12-0-1213419T07:28:50.913Z\t2016-05-02T20:20:00.000Z\n" +
                "-9\t0.000785129319\t0.6467\t91\t-27\tLOFJ\tfalse\tBZ\t-6\t148894756-01-1116531T01:00:11.703Z\t2016-05-02T21:20:00.000Z\n" +
                "7\t356.000000000000\t0.5397\t85\t6\tHRUEDRQ\ttrue\tKK\t10\t52554917-01-394091T23:37:05.955Z\t2016-05-02T22:20:00.000Z\n" +
                "-2\t0.043181171641\t0.3539\t13\t26\tWLP\tfalse\tAX\t3\t000-113381838-12-0-850322T20:22:54.045Z\t2016-05-02T23:20:00.000Z\n" +
                "11\t544.000000000000\t0.4989\t53\t-11\tUOJ\ttrue\tKK\t2\t260180008-12-1950808T23:00:05.032Z\t2016-05-03T00:20:00.000Z\n" +
                "0\t-939.150390625000\t0.9357\t14\t9\t\tfalse\tXX\t15\t127992393-01-959717T16:59:35.695Z\t2016-05-03T01:20:00.000Z\n" +
                "-3\t-512.000000000000\t0.7342\t-113\t5\tLOFJ\ttrue\tAX\t-6\t000-178976059-01-0-1341952T01:51:12.126Z\t2016-05-03T02:20:00.000Z\n" +
                "8\t-608.000000000000\t0.2735\t32\t-29\tBE\ttrue\tBZ\t-15\t000-254260040-01-0-1906511T17:20:11.048Z\t2016-05-03T03:20:00.000Z\n" +
                "13\t880.000000000000\t0.8617\t5\t25\tUOJ\ttrue\tBZ\t-1\t129687651-12-972299T17:39:48.572Z\t2016-05-03T04:20:00.000Z\n" +
                "2\t0.000000073780\t0.3799\t117\t-2\t\tfalse\tKK\t4\t000-121719948-12-0-912521T23:56:05.720Z\t2016-05-03T05:20:00.000Z\n" +
                "4\t203.186584472656\t0.2607\t71\t0\tLOFJ\ttrue\tUX\t17\t000-181321963-12-0-1359876T22:04:02.927Z\t2016-05-03T06:20:00.000Z\n" +
                "-14\t786.767028808594\t0.3143\t-5\t-17\tHRUEDRQ\tfalse\tAX\t0\t259734789-12-1947350T21:53:22.027Z\t2016-05-03T07:20:00.000Z\n" +
                "-4\t0.000002205143\t0.5756\t118\t2\t\tfalse\tXX\t-1\t000-276915595-12-0-2076643T14:11:20.192Z\t2016-05-03T08:20:00.000Z\n" +
                "-4\t-5.687500000000\t0.7677\t-54\t10\tJWCPSWHY\ttrue\tUX\t-13\t54011371-01-405056T08:46:17.912Z\t2016-05-03T09:20:00.000Z\n" +
                "0\t-1018.312500000000\t0.5689\t31\t11\tLOFJ\ttrue\tPP\t18\t000-180048954-12-0-1350005T11:14:15.944Z\t2016-05-03T10:20:00.000Z\n" +
                "1\t380.435256958008\t0.5316\t-34\t24\tJWCPSWHY\tfalse\tUX\t-4\t000-278213384-12-0-2086432T21:56:31.179Z\t2016-05-03T11:20:00.000Z\n" +
                "0\t-435.875000000000\t0.7209\t-119\t-5\tHRUEDRQ\tfalse\tUX\t6\t242190180-01-1816340T13:33:39.965Z\t2016-05-03T12:20:00.000Z\n" +
                "7\t128.000000000000\t0.4951\t25\t29\tGZSXUXI\tfalse\tKK\t16\t000-278791443-01-0-2090265T22:56:32.254Z\t2016-05-03T13:20:00.000Z\n" +
                "-9\t-1024.000000000000\t0.1471\t-13\t-29\tGPGWFFYU\ttrue\tAX\t-9\t000-236713650-12-0-1775100T19:13:46.659Z\t2016-05-03T14:20:00.000Z\n" +
                "8\t0.000000150060\t0.1841\t117\t22\tGZSXUXI\tfalse\tAX\t15\t000-238637570-01-0-1789402T04:15:24.913Z\t2016-05-03T15:20:00.000Z\n" +
                "0\t0.397523656487\t0.6872\t-47\t28\t\tfalse\tXX\t5\t000-111730444-01-0-837455T23:35:39.795Z\t2016-05-03T16:20:00.000Z\n" +
                "-7\t0.000000000000\t0.6077\t55\t5\t\ttrue\tAX\t11\t000-203334702-12-0-1524779T12:38:49.911Z\t2016-05-03T17:20:00.000Z\n" +
                "4\t0.696862459183\t0.8342\t100\t-25\tXY\ttrue\tBZ\t2\t000-225796734-01-0-1692938T17:26:22.000Z\t2016-05-03T18:20:00.000Z\n" +
                "-3\t630.071426391602\t0.5040\t-104\t22\tGZSXUXI\ttrue\tLK\t19\t000-245693006-01-0-1842154T18:07:37.692Z\t2016-05-03T19:20:00.000Z\n" +
                "9\t0.000011650457\t0.4842\t-10\t-26\tHRUEDRQ\ttrue\tLK\t10\t155426653-01-1165418T12:20:05.600Z\t2016-05-03T20:20:00.000Z\n" +
                "13\t-486.985961914063\t0.8112\t17\t1\tUOJ\ttrue\tUX\t-1\t000-164970213-01-0-1236734T08:29:03.419Z\t2016-05-03T21:20:00.000Z\n" +
                "-5\t0.000001392326\t0.4775\t70\t-1\tF\tfalse\tBZ\t19\t264179902-01-1981293T15:17:03.075Z\t2016-05-03T22:20:00.000Z\n" +
                "0\t0.000115693385\t0.4571\t-80\t27\tF\tfalse\tAX\t-3\t111348107-12-834738T05:03:18.261Z\t2016-05-03T23:20:00.000Z\n" +
                "13\t199.865051269531\t0.5774\t99\t-24\tUOJ\ttrue\tXX\t-17\t275822529-01-2068525T06:47:48.447Z\t2016-05-04T00:20:00.000Z\n" +
                "5\t342.142333984375\t0.9537\t-120\t-24\tEYYQEHB\tfalse\tAX\t-12\t000-241414391-12-0-1810354T23:17:30.016Z\t2016-05-04T01:20:00.000Z\n" +
                "-10\t0.000001694924\t0.2260\t-75\t9\tBE\ttrue\tPP\t15\t184910793-12-1386146T15:23:59.555Z\t2016-05-04T02:20:00.000Z\n" +
                "14\t-590.250000000000\t0.3742\t-87\t-21\tLOFJ\ttrue\tAX\t11\t276703961-01-2074910T08:14:52.412Z\t2016-05-04T03:20:00.000Z\n" +
                "0\t-966.000000000000\t0.1861\t-13\t-10\tEYYQEHB\ttrue\tXX\t-15\t251106592-01-1882931T12:15:33.296Z\t2016-05-04T04:20:00.000Z\n" +
                "-13\t796.500000000000\t0.3671\t126\t-28\tF\tfalse\tLK\t-1\t199129681-12-1492997T06:18:53.598Z\t2016-05-04T05:20:00.000Z\n" +
                "14\t0.000002621292\t0.4290\t-77\t11\tF\ttrue\tAX\t-3\t000-39200936-12-0-293812T17:21:59.836Z\t2016-05-04T06:20:00.000Z\n" +
                "1\t-930.523193359375\t0.3797\t-3\t-28\tXPEHNR\ttrue\tBZ\t8\t149040704-01-1117575T00:17:04.693Z\t2016-05-04T07:20:00.000Z\n" +
                "-12\t312.000000000000\t0.3777\t34\t-17\tEYYQEHB\tfalse\tBZ\t-18\t151106452-12-1132677T23:07:51.066Z\t2016-05-04T08:20:00.000Z\n" +
                "6\t0.000000032774\t0.5188\t-41\t-28\tWLP\ttrue\tBZ\t-11\t283656749-01-2127343T09:19:02.551Z\t2016-05-04T09:20:00.000Z\n" +
                "9\t19.849394798279\t0.7422\t63\t-18\tWLP\tfalse\tPP\t15\t218141935-12-1635401T17:36:19.271Z\t2016-05-04T10:20:00.000Z\n";

        assertThat(expected, "parent");
    }

    @Test
    public void testSelectedColumnsRowId() throws Exception {
        assertRowId("select date, sym from parent", "date");
    }

    @Test
    public void testTopRowId() throws Exception {
        assertRowId("parent limit 0,10", "timestamp");
    }

    @Test
    public void testVanillaRowId() throws Exception {
        assertRowId("parent", "date");
    }

    @Test
    public void testVirtualColumnRowId() throws Exception {
        assertRowId("select date, d + f from parent", "date");
    }
}