/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.ha;

import com.nfsdb.*;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.ha.config.ServerNode;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.storage.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GenericTest extends AbstractTest {

    @Test
    public void testGenericPublish() throws Exception {

        JournalWriter w = factory.writer(new JournalStructure("xyz") {{
            $sym("x").index();
            $int("y");
            $double("z");
            $ts();
            partitionBy(PartitionType.DAY);
        }});

        JournalServer server = new JournalServer(new ServerConfig() {{
            addNode(new ServerNode(1, "localhost"));
            setHeartbeatFrequency(100);
        }}, factory);
        server.publish(w);
        server.start();

        final CountDownLatch ready = new CountDownLatch(1);

        JournalClient client = new JournalClient(new ClientConfig("localhost"), factory);
        client.subscribe(new JournalKey("xyz"), new JournalKey("abc"), new TxListener() {
            @Override
            public void onCommit() {
                ready.countDown();
            }

            @Override
            public void onError() {

            }
        });
        client.start();

        Rnd rnd = new Rnd();
        for (int i = 0; i < 100; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putSym(0, rnd.nextString(10));
            ew.putInt(1, rnd.nextInt());
            ew.putDouble(2, rnd.nextDouble());
            ew.append();
        }
        w.commit();

        ready.await(1, TimeUnit.SECONDS);

        Journal r = factory.reader("abc");
        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.print(r.rows());

        final String expected = "VTJWCPSWHY\t-1191262516\t0.024494420737\t1970-01-01T00:00:00.000Z\n" +
                "EHNRXGZSXU\t-1458132197\t768.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "BTGPGWFFYU\t-1125169127\t188.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "YQEHBHFOWL\t-938514914\t0.032379742712\t1970-01-01T00:00:00.000Z\n" +
                "YSBEOUOJSH\t1890602616\t0.000000003895\t1970-01-01T00:00:00.000Z\n" +
                "DRQQULOFJG\t-530317703\t1.583955645561\t1970-01-01T00:00:00.000Z\n" +
                "RSZSRYRFBV\t-1787109293\t0.000076281818\t1970-01-01T00:00:00.000Z\n" +
                "GOOZZVDZJM\t-1212175298\t11.229226589203\t1970-01-01T00:00:00.000Z\n" +
                "CXZOUICWEK\t1876812930\t153.473033905029\t1970-01-01T00:00:00.000Z\n" +
                "UVSDOTSEDY\t-998315423\t56.593492507935\t1970-01-01T00:00:00.000Z\n" +
                "GQOLYXWCKY\t-2075675260\t0.060836097226\t1970-01-01T00:00:00.000Z\n" +
                "WDSWUGSHOL\t1864113037\t-1013.467773437500\t1970-01-01T00:00:00.000Z\n" +
                "IQBZXIOVIK\t519895483\t0.053118304349\t1970-01-01T00:00:00.000Z\n" +
                "SSUQSRLTKV\t-2080340570\t0.000000002016\t1970-01-01T00:00:00.000Z\n" +
                "OJIPHZEPIH\t1362833895\t142.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "OVLJUMLGLH\t923501161\t364.462486267090\t1970-01-01T00:00:00.000Z\n" +
                "EOYPHRIPZI\t-1280991111\t0.451262347400\t1970-01-01T00:00:00.000Z\n" +
                "ZRMFMBEZGH\t-1121895896\t0.001741337735\t1970-01-01T00:00:00.000Z\n" +
                "KFLOPJOXPK\t-1542366041\t-1024.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "IHYHBOQMYS\t39497392\t26.612817287445\t1970-01-01T00:00:00.000Z\n" +
                "GLUOHNZHZS\t-147343840\t0.017535420135\t1970-01-01T00:00:00.000Z\n" +
                "GLOGIFOUSZ\t-1775036711\t-911.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "QEBNDCQCEH\t1448081412\t0.000000001871\t1970-01-01T00:00:00.000Z\n" +
                "VELLKKHTWN\t1147642496\t0.000969694171\t1970-01-01T00:00:00.000Z\n" +
                "FLRBROMNXK\t719793244\t0.000000005098\t1970-01-01T00:00:00.000Z\n" +
                "ULIGYVFZFK\t976011946\t0.000036501544\t1970-01-01T00:00:00.000Z\n" +
                "UOGXHFVWSW\t-1300367617\t488.093750000000\t1970-01-01T00:00:00.000Z\n" +
                "OONFCLTJCK\t-799774729\t0.000043022766\t1970-01-01T00:00:00.000Z\n" +
                "NTOGMXUKLG\t1746137611\t0.000000038730\t1970-01-01T00:00:00.000Z\n" +
                "LUQDYOPHNI\t534328386\t-655.625000000000\t1970-01-01T00:00:00.000Z\n" +
                "FDTNPHFLPB\t992057087\t-830.895019531250\t1970-01-01T00:00:00.000Z\n" +
                "ZWWCCNGTNL\t-857795778\t-353.683593750000\t1970-01-01T00:00:00.000Z\n" +
                "UHHIUGGLNY\t-1341091541\t0.000001089423\t1970-01-01T00:00:00.000Z\n" +
                "CBDMIGQZVK\t1194691156\t103.519111633301\t1970-01-01T00:00:00.000Z\n" +
                "QZSLQVFGPP\t-770962341\t0.003051488020\t1970-01-01T00:00:00.000Z\n" +
                "XBHYSBQYMI\t1289699549\t5.336447119713\t1970-01-01T00:00:00.000Z\n" +
                "VTNPIWZNFK\t-460860589\t0.000012560774\t1970-01-01T00:00:00.000Z\n" +
                "MCGFNWGRMD\t705091880\t0.000000006800\t1970-01-01T00:00:00.000Z\n" +
                "JYDVRVNGST\t855975978\t46.725388526917\t1970-01-01T00:00:00.000Z\n" +
                "DRZEIWFOQK\t481303173\t38.688202857971\t1970-01-01T00:00:00.000Z\n" +
                "QUWQOEENNE\t-970294725\t832.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "EMXDKXEJCT\t-795877457\t768.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "YFLUHZQSNP\t1141437597\t301.691406250000\t1970-01-01T00:00:00.000Z\n" +
                "JSMKIXEYVT\t-534339619\t0.000002205143\t1970-01-01T00:00:00.000Z\n" +
                "HHGGIWHPZR\t-69279231\t0.383871749043\t1970-01-01T00:00:00.000Z\n" +
                "GZJYYFLSVI\t-1354963681\t106.662109375000\t1970-01-01T00:00:00.000Z\n" +
                "WLEVMLKCJB\t1204423553\t0.000118756758\t1970-01-01T00:00:00.000Z\n" +
                "UHLIHYBTVZ\t-1023760162\t0.000001439041\t1970-01-01T00:00:00.000Z\n" +
                "NXFSUWPNXH\t882350590\t0.017423127312\t1970-01-01T00:00:00.000Z\n" +
                "ZODWKOCPFY\t-1558709522\t640.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "KNCBWLNLRH\t-1422542921\t-354.286376953125\t1970-01-01T00:00:00.000Z\n" +
                "YPOVFDBZWN\t1927063457\t-384.616943359375\t1970-01-01T00:00:00.000Z\n" +
                "EHRUGPBMBT\t996323284\t-1024.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "BEGMITINLK\t856634079\t0.000002211302\t1970-01-01T00:00:00.000Z\n" +
                "HNRJUEBWVL\t1890990613\t-486.985961914063\t1970-01-01T00:00:00.000Z\n" +
                "BETTTKRIVO\t-1001120776\t0.238771528006\t1970-01-01T00:00:00.000Z\n" +
                "PUNEFIVQFN\t1820147632\t-162.500000000000\t1970-01-01T00:00:00.000Z\n" +
                "SBOSEPGIUQ\t2146422524\t10.085297346115\t1970-01-01T00:00:00.000Z\n" +
                "ISQHNOJIGF\t-2139920832\t-256.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "GQVZWEVQTQ\t-2129399613\t-492.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "XTPNHTDCEB\t1944405123\t0.047431875020\t1970-01-01T00:00:00.000Z\n" +
                "BBZVRLPTYX\t882405723\t0.000005221712\t1970-01-01T00:00:00.000Z\n" +
                "FUXCDKDWOM\t1226884727\t-215.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "BJFRPXZSFX\t-483085744\t-1024.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "QXTGNJJILL\t-481534978\t1.034002423286\t1970-01-01T00:00:00.000Z\n" +
                "IWTCWLFORG\t1733247129\t19.849394798279\t1970-01-01T00:00:00.000Z\n" +
                "VMKPYVGPYK\t12659434\t-948.018188476563\t1970-01-01T00:00:00.000Z\n" +
                "QMUDDCIHCN\t880291989\t-1024.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "JOPJEUKWMD\t-592205337\t-886.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "BBUKOJSOLD\t181870148\t0.014446553774\t1970-01-01T00:00:00.000Z\n" +
                "DIPUNRPSMI\t233670179\t0.000001588220\t1970-01-01T00:00:00.000Z\n" +
                "PDKOEZBRQS\t-1312915365\t-736.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "DIHHNSSTCR\t1881940349\t2.266549527645\t1970-01-01T00:00:00.000Z\n" +
                "VQFULMERTP\t767949332\t-960.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "UYZVQQHSQS\t-459351439\t5.076923489571\t1970-01-01T00:00:00.000Z\n" +
                "BHLNEJRMDI\t-877688809\t987.093750000000\t1970-01-01T00:00:00.000Z\n" +
                "SGQFYQPZGP\t2091979674\t0.000000078822\t1970-01-01T00:00:00.000Z\n" +
                "VLTPKBBQFN\t-259645664\t0.000000042444\t1970-01-01T00:00:00.000Z\n" +
                "NNCTFSNSXH\t-485950131\t2.123810946941\t1970-01-01T00:00:00.000Z\n" +
                "LELRUMMZSC\t1124318483\t0.000124199963\t1970-01-01T00:00:00.000Z\n" +
                "OUIGENFELW\t-2031363046\t242.145568847656\t1970-01-01T00:00:00.000Z\n" +
                "LBMQHGJBFQ\t264877675\t0.000000002151\t1970-01-01T00:00:00.000Z\n" +
                "FIJZZYNPPB\t1344590222\t445.768707275391\t1970-01-01T00:00:00.000Z\n" +
                "VRIIYMHOWK\t-764794651\t0.000000542615\t1970-01-01T00:00:00.000Z\n" +
                "ZNLCNGZTOY\t-74511843\t512.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "RSFPVRQLGY\t-560545477\t0.000000971470\t1970-01-01T00:00:00.000Z\n" +
                "NLITWGLFCY\t-992831440\t0.119503878057\t1970-01-01T00:00:00.000Z\n" +
                "KLHTIIGQEY\t-485549973\t-448.039062500000\t1970-01-01T00:00:00.000Z\n" +
                "VRGRQGKNPH\t1946796084\t0.000000093964\t1970-01-01T00:00:00.000Z\n" +
                "BVDEGHLXGZ\t-420096736\t0.000084972578\t1970-01-01T00:00:00.000Z\n" +
                "THMHZNVZHC\t1870334962\t2.253738045692\t1970-01-01T00:00:00.000Z\n" +
                "EQGMPLUCFT\t-1073362485\t0.075787898153\t1970-01-01T00:00:00.000Z\n" +
                "YTSZEOCVFF\t712201059\t0.012697421480\t1970-01-01T00:00:00.000Z\n" +
                "KPFOYMNWDS\t80777671\t388.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "VDRHFBCZIO\t-1534034235\t-272.000000000000\t1970-01-01T00:00:00.000Z\n" +
                "PGZHITQJLK\t1143795193\t100.419076919556\t1970-01-01T00:00:00.000Z\n" +
                "LVSYLMSRHG\t-535358959\t41.351981163025\t1970-01-01T00:00:00.000Z\n" +
                "KUSIMYDXUU\t157435167\t5.152941465378\t1970-01-01T00:00:00.000Z\n" +
                "XNMUREIJUH\t1563307851\t433.924621582031\t1970-01-01T00:00:00.000Z\n" +
                "CMZCCYVBDM\t-1982292415\t216.046455383301\t1970-01-01T00:00:00.000Z\n";

        Assert.assertEquals(expected, sink.toString());

        client.halt();
        server.halt();
    }
}
