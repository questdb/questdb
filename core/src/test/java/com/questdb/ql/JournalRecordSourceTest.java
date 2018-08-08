/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.std.Rnd;
import com.questdb.std.time.Dates;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.BeforeClass;
import org.junit.Test;

public class JournalRecordSourceTest extends AbstractOptimiserTest {
    @BeforeClass
    public static void setUp() throws Exception {
        try (JournalWriter w = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("parent")
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
        final String expected = "5\t0.047392679378\t0.3350\t-127\t3\tHRUEDRQ\ttrue\tUX\t18\t-279216330-12-02T14:28:07.160Z\t2016-05-01T11:20:00.000Z\n" +
                "-14\t-284.232421875000\t0.4112\t-48\t17\t\tfalse\tLK\t4\t258937948-03-11T22:24:08.944Z\t2016-05-01T12:20:00.000Z\n" +
                "-10\t-512.000000000000\t0.7163\t-79\t12\tHRUEDRQ\ttrue\tBZ\t-2\t185889770-02-23T18:31:09.514Z\t2016-05-01T13:20:00.000Z\n" +
                "-3\t424.828125000000\t0.2858\t55\t-2\tGZSXUXI\ttrue\tAX\t-5\t201963330-01-06T09:59:49.837Z\t2016-05-01T14:20:00.000Z\n" +
                "5\t0.000000009480\t0.8258\t-61\t23\tEYYQEHB\ttrue\tPP\t-3\t258454748-03-10T09:05:26.575Z\t2016-05-01T15:20:00.000Z\n" +
                "8\t557.000000000000\t0.5172\t-41\t-21\tF\tfalse\tAX\t-4\t47692444-11-24T00:16:53.191Z\t2016-05-01T16:20:00.000Z\n" +
                "-10\t0.060836097226\t0.6644\t-123\t1\t\ttrue\tLK\t2\t29025895-08-22T23:29:37.949Z\t2016-05-01T17:20:00.000Z\n" +
                "-3\t300.163238525391\t0.5051\t-109\t-22\t\tfalse\tUX\t-17\t-172370722-08-30T13:59:33.528Z\t2016-05-01T18:20:00.000Z\n" +
                "-13\t0.107344331220\t0.4957\t-69\t-22\t\tfalse\tXX\t4\t-282363803-02-23T22:07:48.466Z\t2016-05-01T19:20:00.000Z\n" +
                "12\t0.248532287776\t0.4957\t-88\t23\tXY\tfalse\tPP\t13\t-259642767-03-23T05:28:26.667Z\t2016-05-01T20:20:00.000Z\n" +
                "12\t-583.609375000000\t0.6253\t56\t-15\tEYYQEHB\ttrue\tKK\t0\t-156780555-03-14T20:51:19.305Z\t2016-05-01T21:20:00.000Z\n" +
                "8\t-864.392822265625\t0.7237\t-62\t19\tEYYQEHB\tfalse\tPP\t-6\t147076965-11-19T02:54:29.993Z\t2016-05-01T22:20:00.000Z\n" +
                "0\t-119.000000000000\t0.3158\t-55\t-23\tHRUEDRQ\tfalse\tXX\t14\t253798344-05-11T01:48:28.243Z\t2016-05-01T23:20:00.000Z\n" +
                "-2\t-352.000000000000\t0.5488\t29\t-9\t\tfalse\tPP\t5\t-285016098-11-30T04:17:15.224Z\t2016-05-02T00:20:00.000Z\n" +
                "9\t134.663085937500\t0.5315\t-91\t-16\tGZSXUXI\tfalse\tPP\t-3\t245893755-08-11T03:30:38.709Z\t2016-05-02T01:20:00.000Z\n" +
                "3\t0.010311927646\t0.4648\t68\t17\tGPGWFFYU\ttrue\tKK\t-16\t-226689458-05-22T21:25:17.104Z\t2016-05-02T02:20:00.000Z\n" +
                "-11\t0.000000032913\t0.3010\t98\t-4\t\ttrue\tBZ\t-9\t75525295-09-06T22:11:27.250Z\t2016-05-02T03:20:00.000Z\n" +
                "-10\t-144.421875000000\t0.5373\t46\t8\tGPGWFFYU\tfalse\tXX\t0\t-256539251-11-26T13:36:52.642Z\t2016-05-02T04:20:00.000Z\n" +
                "7\t0.000000064758\t0.4588\t44\t14\tUOJ\tfalse\tBZ\t-12\t223665704-03-23T09:36:06.226Z\t2016-05-02T05:20:00.000Z\n" +
                "-11\t-911.000000000000\t0.3472\t39\t-15\tLOFJ\ttrue\tLK\t-10\t-260012340-07-11T16:21:52.892Z\t2016-05-02T06:20:00.000Z\n" +
                "13\t-589.718750000000\t0.0314\t12\t26\tXPEHNR\ttrue\tBZ\t9\t219842116-11-17T17:57:00.303Z\t2016-05-02T07:20:00.000Z\n" +
                "-11\t1.167305707932\t0.5309\t-128\t3\t\tfalse\tAX\t6\t-141025066-04-05T08:09:18.874Z\t2016-05-02T08:20:00.000Z\n" +
                "-11\t15.713242053986\t0.5207\t58\t10\tUOJ\tfalse\tAX\t19\t63572238-04-24T11:00:13.287Z\t2016-05-02T09:20:00.000Z\n" +
                "-12\t296.611343383789\t0.2628\t-48\t17\tGPGWFFYU\tfalse\tAX\t14\t-216113547-08-09T23:21:50.616Z\t2016-05-02T10:20:00.000Z\n" +
                "-14\t0.000000248992\t0.5233\t92\t28\t\tfalse\tKK\t-16\t121072971-06-08T04:40:57.855Z\t2016-05-02T11:20:00.000Z\n" +
                "-6\t0.000014454320\t0.5999\t-77\t-6\tHRUEDRQ\tfalse\tPP\t8\t214009105-03-25T03:32:20.665Z\t2016-05-02T12:20:00.000Z\n" +
                "-14\t0.000000119284\t0.2506\t-12\t-15\tLOFJ\tfalse\tPP\t-12\t119515869-09-19T20:43:31.509Z\t2016-05-02T13:20:00.000Z\n" +
                "-14\t7.793050527573\t0.3849\t-109\t-15\tWLP\ttrue\tUX\t-7\t-234459915-01-31T17:49:54.678Z\t2016-05-02T14:20:00.000Z\n" +
                "-3\t0.001914593857\t0.2802\t46\t-6\tGPGWFFYU\tfalse\tLK\t-17\t-271112442-06-04T20:33:53.536Z\t2016-05-02T15:20:00.000Z\n" +
                "-12\t0.000000001910\t0.3988\t-79\t-8\tWLP\ttrue\tPP\t15\t183931713-08-27T07:01:08.360Z\t2016-05-02T16:20:00.000Z\n" +
                "6\t0.000000000000\t0.3820\t-49\t5\tGZSXUXI\tfalse\tBZ\t-18\t-197160329-04-12T04:43:10.644Z\t2016-05-02T17:20:00.000Z\n" +
                "4\t0.000000000000\t0.4107\t-5\t19\t\tfalse\tLK\t2\t262468290-01-10T09:15:31.151Z\t2016-05-02T18:20:00.000Z\n" +
                "-11\t0.000001089423\t0.2162\t-43\t15\tLOFJ\tfalse\tPP\t0\t209089147-12-19T04:29:10.493Z\t2016-05-02T19:20:00.000Z\n" +
                "0\t0.000289257550\t0.4044\t42\t0\tHRUEDRQ\tfalse\tAX\t10\t-161826133-09-07T07:28:50.913Z\t2016-05-02T20:20:00.000Z\n" +
                "-9\t0.000785129319\t0.6467\t91\t-27\tLOFJ\tfalse\tBZ\t-6\t148897812-12-15T01:00:11.703Z\t2016-05-02T21:20:00.000Z\n" +
                "7\t356.000000000000\t0.5397\t85\t6\tHRUEDRQ\ttrue\tKK\t10\t52555995-12-26T23:37:05.955Z\t2016-05-02T22:20:00.000Z\n" +
                "-2\t0.043181171641\t0.3539\t13\t26\tWLP\tfalse\tAX\t3\t-113384166-10-23T20:22:54.045Z\t2016-05-02T23:20:00.000Z\n" +
                "11\t544.000000000000\t0.4989\t53\t-11\tUOJ\ttrue\tKK\t2\t260185350-01-17T23:00:05.032Z\t2016-05-03T00:20:00.000Z\n" +
                "0\t-939.150390625000\t0.9357\t14\t9\t\tfalse\tXX\t15\t127995020-08-13T16:59:35.695Z\t2016-05-03T01:20:00.000Z\n" +
                "-3\t-512.000000000000\t0.7342\t-113\t5\tLOFJ\ttrue\tAX\t-6\t-178979734-11-09T01:51:12.126Z\t2016-05-03T02:20:00.000Z\n" +
                "8\t-608.000000000000\t0.2735\t32\t-29\tBE\ttrue\tBZ\t-15\t-254265260-02-23T17:20:11.048Z\t2016-05-03T03:20:00.000Z\n" +
                "13\t880.000000000000\t0.8617\t5\t25\tUOJ\ttrue\tBZ\t-1\t129690313-12-24T17:39:48.572Z\t2016-05-03T04:20:00.000Z\n" +
                "2\t0.000000073780\t0.3799\t117\t-2\t\tfalse\tKK\t4\t-121722446-07-08T23:56:05.720Z\t2016-05-03T05:20:00.000Z\n" +
                "4\t203.186584472656\t0.2607\t71\t0\tLOFJ\ttrue\tUX\t17\t-181325686-09-13T22:04:02.927Z\t2016-05-03T06:20:00.000Z\n" +
                "-14\t786.767028808594\t0.3143\t-5\t-17\tHRUEDRQ\tfalse\tAX\t0\t259740121-07-30T21:53:22.027Z\t2016-05-03T07:20:00.000Z\n" +
                "-4\t0.000002205143\t0.5756\t118\t2\t\tfalse\tXX\t-1\t-276921280-04-04T14:11:20.192Z\t2016-05-03T08:20:00.000Z\n" +
                "-4\t-5.687500000000\t0.7677\t-54\t10\tJWCPSWHY\ttrue\tUX\t-13\t54012480-01-02T08:46:17.912Z\t2016-05-03T09:20:00.000Z\n" +
                "0\t-1018.312500000000\t0.5689\t31\t11\tLOFJ\ttrue\tPP\t18\t-180052650-09-21T11:14:15.944Z\t2016-05-03T10:20:00.000Z\n" +
                "1\t380.435256958008\t0.5316\t-34\t24\tJWCPSWHY\tfalse\tUX\t-4\t-278219096-06-15T21:56:31.179Z\t2016-05-03T11:20:00.000Z\n" +
                "0\t-435.875000000000\t0.7209\t-119\t-5\tHRUEDRQ\tfalse\tUX\t6\t242195152-12-20T13:33:39.965Z\t2016-05-03T12:20:00.000Z\n" +
                "7\t128.000000000000\t0.4951\t25\t29\tGZSXUXI\tfalse\tKK\t16\t-278797166-01-17T22:56:32.254Z\t2016-05-03T13:20:00.000Z\n" +
                "-9\t-1024.000000000000\t0.1471\t-13\t-29\tGPGWFFYU\ttrue\tAX\t-9\t-236718510-11-07T19:13:46.659Z\t2016-05-03T14:20:00.000Z\n" +
                "8\t0.000000150060\t0.1841\t117\t22\tGZSXUXI\tfalse\tAX\t15\t-238642470-10-13T04:15:24.913Z\t2016-05-03T15:20:00.000Z\n" +
                "0\t0.397523656487\t0.6872\t-47\t28\t\tfalse\tXX\t5\t-111732737-02-13T23:35:39.795Z\t2016-05-03T16:20:00.000Z\n" +
                "-7\t0.000000000000\t0.6077\t55\t5\t\ttrue\tAX\t11\t-203338876-03-18T12:38:49.911Z\t2016-05-03T17:20:00.000Z\n" +
                "4\t0.696862459183\t0.8342\t100\t-25\tXY\ttrue\tBZ\t2\t-225801370-11-22T17:26:22.000Z\t2016-05-03T18:20:00.000Z\n" +
                "-3\t630.071426391602\t0.5040\t-104\t22\tGZSXUXI\ttrue\tLK\t19\t-245698050-05-09T18:07:37.692Z\t2016-05-03T19:20:00.000Z\n" +
                "9\t0.000011650457\t0.4842\t-10\t-26\tHRUEDRQ\ttrue\tLK\t10\t155429843-10-22T12:20:05.600Z\t2016-05-03T20:20:00.000Z\n" +
                "13\t-486.985961914063\t0.8112\t17\t1\tUOJ\ttrue\tUX\t-1\t-164973600-12-07T08:29:03.419Z\t2016-05-03T21:20:00.000Z\n" +
                "-5\t0.000001392326\t0.4775\t70\t-1\tF\tfalse\tBZ\t19\t264185326-08-05T15:17:03.075Z\t2016-05-03T22:20:00.000Z\n" +
                "0\t0.000115693385\t0.4571\t-80\t27\tF\tfalse\tAX\t-3\t111350393-05-07T05:03:18.261Z\t2016-05-03T23:20:00.000Z\n" +
                "13\t199.865051269531\t0.5774\t99\t-24\tUOJ\ttrue\tXX\t-17\t275828192-06-05T06:47:48.447Z\t2016-05-04T00:20:00.000Z\n" +
                "5\t342.142333984375\t0.9537\t-120\t-24\tEYYQEHB\tfalse\tAX\t-12\t-241419347-05-01T23:17:30.016Z\t2016-05-04T01:20:00.000Z\n" +
                "-10\t0.000001694924\t0.2260\t-75\t9\tBE\ttrue\tPP\t15\t184914589-01-19T15:23:59.555Z\t2016-05-04T02:20:00.000Z\n" +
                "14\t-590.250000000000\t0.3742\t-87\t-21\tLOFJ\ttrue\tAX\t11\t276709641-11-28T08:14:52.412Z\t2016-05-04T03:20:00.000Z\n" +
                "0\t-966.000000000000\t0.1861\t-13\t-10\tEYYQEHB\ttrue\tXX\t-15\t251111747-04-16T12:15:33.296Z\t2016-05-04T04:20:00.000Z\n" +
                "-13\t796.500000000000\t0.3671\t126\t-28\tF\tfalse\tLK\t-1\t199133769-08-08T06:18:53.598Z\t2016-05-04T05:20:00.000Z\n" +
                "14\t0.000002621292\t0.4290\t-77\t11\tF\ttrue\tAX\t-3\t-39201740-06-25T17:21:59.836Z\t2016-05-04T06:20:00.000Z\n" +
                "1\t-930.523193359375\t0.3797\t-3\t-28\tXPEHNR\ttrue\tBZ\t8\t149043763-10-24T00:17:04.693Z\t2016-05-04T07:20:00.000Z\n" +
                "-12\t312.000000000000\t0.3777\t34\t-17\tEYYQEHB\tfalse\tBZ\t-18\t151109554-01-30T23:07:51.066Z\t2016-05-04T08:20:00.000Z\n" +
                "6\t0.000000032774\t0.5188\t-41\t-28\tWLP\ttrue\tBZ\t-11\t283662573-06-19T09:19:02.551Z\t2016-05-04T09:20:00.000Z\n" +
                "9\t19.849394798279\t0.7422\t63\t-18\tWLP\tfalse\tPP\t15\t218146413-06-27T17:36:19.271Z\t2016-05-04T10:20:00.000Z\n";

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