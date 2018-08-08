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

package com.questdb.net.ha;

import com.questdb.ex.JournalConfigurationException;
import com.questdb.model.Quote;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.ql.RecordSource;
import com.questdb.ql.RecordSourcePrinter;
import com.questdb.std.Rnd;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.*;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GenericTest extends AbstractTest {

    @Test
    public void testClassToGenericPublish() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote")) {

            JournalServer server = new JournalServer(new ServerConfig() {{
                addNode(new ServerNode(1, "localhost"));
                setHeartbeatFrequency(100);
                setEnableMultiCast(false);
            }}, getFactory());

            server.publish(w);
            server.start();

            final CountDownLatch ready = new CountDownLatch(1);

            JournalClient client = new JournalClient(new ClientConfig("localhost") {{
                setEnableMultiCast(false);
            }}, getFactory());

            client.subscribe(new JournalKey("quote"), new JournalKey("abc"), new JournalListener() {
                @Override
                public void onCommit() {
                    ready.countDown();
                }

                @Override
                public void onEvent(int event) {

                }
            });
            client.start();

            TestUtils.generateQuoteData(w, 100, DateFormatUtils.parseDateTime("2015-01-10T12:00:00.000Z"));
            w.commit();

            Assert.assertTrue(ready.await(1, TimeUnit.SECONDS));

            StringSink sink = new StringSink();
            RecordSourcePrinter p = new RecordSourcePrinter(sink);
            try (RecordSource rs = compile("abc")) {
                p.print(rs, getFactory());

                final String expected = "2015-01-10T12:00:00.000Z\tAGK.L\t0.000001189157\t1.050231933594\t1326447242\t948263339\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t104.021850585938\t0.006688738358\t1575378703\t1436881714\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t879.117187500000\t496.806518554688\t1530831067\t339631474\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t768.000000000000\t0.000020634160\t426455968\t1432278050\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t256.000000000000\t0.000000035797\t1404198\t1153445279\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t920.625000000000\t0.040750414133\t761275053\t1232884790\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t512.000000000000\t896.000000000000\t422941535\t113506296\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t12.923866510391\t0.032379742712\t2006313928\t2132716300\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.006530375686\t0.000000000000\t1890602616\t2137969456\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.000000017324\t720.000000000000\t410717394\t458818940\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t384.000000000000\t0.000000019700\t1575135393\t530317703\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.001165474765\t0.000001752813\t171200398\t2034804966\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tWTB.L\t1.507822513580\t695.796875000000\t1515787781\t66297136\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t172.796875000000\t1.959461987019\t360860352\t1538602195\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.000000006081\t942.899414062500\t1857212401\t1985398001\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t424.828125000000\t1024.000000000000\t1269042121\t1566901076\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t153.473033905029\t0.000355183205\t532665695\t1424048819\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t632.921875000000\t348.175781250000\t89906802\t373499303\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t186.000000000000\t0.000020114637\t731466113\t1609750740\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.015470010694\t671.442138671875\t880943673\t1172180184\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t0.000000009901\t0.060836097226\t1235206821\t817130367\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.003103211522\t271.689331054688\t1864113037\t865832060\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t770.359375000000\t0.000000014643\t618037497\t844704299\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t1.229880273342\t4.852988362312\t639125092\t519895483\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tWTB.L\t0.000001305853\t767.380859375000\t614536941\t462277692\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.000000776007\t0.000484068747\t195213883\t283321892\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t583.609375000000\t0.555824235082\t1015055928\t1383560599\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t296.544433593750\t174.774871826172\t1503763988\t805434743\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t842.000000000000\t100.000000000000\t514934130\t246923735\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t364.462486267090\t0.000002526560\t1475953213\t1023667478\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.000603844470\t0.000027837185\t133913299\t2042181314\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t0.000000261681\t352.000000000000\t684778036\t2076507991\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.000125102033\t0.000000000000\t880219256\t862447505\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tWTB.L\t0.000000194258\t0.001741337735\t1204245663\t1179767285\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t218.371459960938\t0.000003607215\t1542366041\t239305284\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t1024.000000000000\t33.643297195435\t2077041000\t2062507031\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t256.000000000000\t0.001175858604\t292438036\t39497392\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t144.421875000000\t0.000063085048\t597366062\t370796356\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t256.000000000000\t0.000000032050\t484276102\t2017314910\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.000080152415\t832.000000000000\t1465751763\t36814604\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t512.000000000000\t69.914062500000\t874367915\t120660220\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t768.000000000000\t134.988502502441\t1218814076\t1822590290\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.000000001871\t589.718750000000\t1484108978\t1962248170\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t736.000000000000\t0.000003953393\t1570930196\t494223693\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t0.000000001374\t0.000000014146\t273567866\t365989785\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t0.115072973073\t3.896593213081\t1180113884\t1863806522\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t386.843750000000\t0.000000005098\t689798930\t529035657\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t5.590153217316\t8.408761024475\t976011946\t1594425659\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.000000248992\t10.084638834000\t557653156\t1440049547\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.000002116648\t164.218750000000\t1479209616\t1300367617\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t0.000005107453\t0.000014963527\t2136017735\t782135501\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t629.480468750000\t310.101058959961\t465171440\t1805101061\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t0.000000049302\t0.611244902015\t297507019\t1197986472\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t672.000000000000\t7.793050527573\t831951785\t2108259867\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t0.018715771381\t1.461132109165\t1561652006\t1915752164\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t655.625000000000\t52.330078125000\t1501720177\t464081554\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t0.000036359392\t121.029296875000\t876817614\t1705668785\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t265.807006835938\t0.004646989168\t1912522421\t1654490571\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t0.971607863903\t97.790924072266\t1436639185\t1975151962\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t0.000000000000\t353.683593750000\t1604266757\t647653731\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t8.039016723633\t7.977778911591\t1341091541\t1977116623\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t638.000000000000\t0.014096791856\t1091570282\t772867311\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.000002654824\t820.224609375000\t2094760568\t1194691156\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t300.093750000000\t0.000000040298\t724165345\t1307291490\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.005052038119\t0.000785129319\t1418612367\t1933125405\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t0.127358488739\t0.000000718634\t570261315\t158323100\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t0.000000009919\t0.000000059185\t1290623970\t873017617\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.040659694001\t0.000000818963\t2029231362\t400205299\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t0.000012560774\t5.638884067535\t1722682901\t1025310386\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t873.000000000000\t127.708919525146\t1753567161\t256128916\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t939.150390625000\t512.000000000000\t447593202\t135792148\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t978.632812500000\t0.000194281980\t1567482693\t8264817\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t844.000000000000\t46.725388526917\t432358603\t1657803999\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t31.000000000000\t428.000000000000\t481303173\t739168384\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tWTB.L\t75.842805862427\t0.000002408446\t1297032488\t1215601315\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t283.158325195313\t880.000000000000\t357119340\t970294725\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.000003385293\t965.151367187500\t844915022\t1889838659\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.031326758675\t0.360216885805\t263487884\t1680503149\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t508.375000000000\t768.000000000000\t1923884740\t1121347399\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t12.290055274963\t0.011763263494\t304383158\t2039181884\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t4.111308574677\t1.257051765919\t159178348\t604818378\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t0.000002205143\t228.001586914063\t1815322506\t1197959281\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tAGK.L\t0.000000129186\t0.011274382472\t1060590724\t1159512064\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t19.756743907928\t0.000340424791\t526923908\t1034870849\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tADM.L\t0.000180012023\t0.000000052629\t1406232957\t378247895\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t400.000000000000\t106.662109375000\t1440131320\t971963578\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tWTB.L\t68.043695449829\t380.435256958008\t1204423553\t795316150\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.004094106262\t0.000000914462\t2138704106\t655076307\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t64.000000000000\t445.875000000000\t1848218326\t1023760162\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBT-A.L\t0.000000157150\t6.012886285782\t1247892921\t1890713369\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t497.000000000000\t111.975021362305\t24972718\t1583707719\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t549.125503540039\t18.834793090820\t957024901\t1487779338\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t0.000000150060\t412.000000000000\t1158267509\t980916820\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tTLW.L\t0.000000007168\t41.500000000000\t1725416460\t1078921487\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tBP.L\t354.286376953125\t540.000000000000\t1772028439\t1496904948\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tABF.L\t0.000010415702\t372.000000000000\t1262895671\t1914805024\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t728.300781250000\t15.240249156952\t1926049591\t2113879331\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tGKN.L\t448.000000000000\t0.696862459183\t1499957018\t1753521575\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tLLOY.L\t0.019178653602\t1024.000000000000\t1445553836\t896453005\tFast trading\tLXE\n" +
                        "2015-01-10T12:00:00.000Z\tRRS.L\t1021.000000000000\t391.000000000000\t856634079\t840591709\tFast trading\tLXE\n";

                Assert.assertEquals(expected, sink.toString());

                client.halt();
                server.halt();
            }
        }
    }

    @Test
    public void testDuplicateTimestamp() throws Exception {
        try {
            getFactory().writer(new JournalStructure("xyz") {{
                $sym("x").index();
                $int("y");
                $double("z");
                $ts("a");
                $ts("b");
                partitionBy(PartitionBy.DAY);
            }}).close();
            Assert.fail();
        } catch (JournalConfigurationException ignore) {
            // pass
        }
    }

    @Test
    public void testGenericPublish() throws Exception {

        try (JournalWriter w = getFactory().writer(new JournalStructure("xyz") {{
            $sym("x").index();
            $int("y");
            $double("z");
            $ts();
            partitionBy(PartitionBy.DAY);
        }})) {

            JournalServer server = new JournalServer(new ServerConfig() {{
                addNode(new ServerNode(1, "localhost"));
                setHeartbeatFrequency(100);
                setEnableMultiCast(false);
            }}, getFactory());
            server.publish(w);
            server.start();

            final CountDownLatch ready = new CountDownLatch(1);

            JournalClient client = new JournalClient(new ClientConfig("localhost") {{
                setEnableMultiCast(false);
            }}, getFactory());
            client.subscribe(new JournalKey("xyz"), new JournalKey("abc"), new JournalListener() {
                @Override
                public void onCommit() {
                    ready.countDown();
                }

                @Override
                public void onEvent(int event) {

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

            Assert.assertTrue(ready.await(1, TimeUnit.SECONDS));

            StringSink sink = new StringSink();
            RecordSourcePrinter p = new RecordSourcePrinter(sink);
            try (RecordSource rs = compile("abc")) {
                p.print(rs, getFactory());

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
    }
}
