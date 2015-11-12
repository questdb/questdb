/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.impl.select.SelectedColumnsRecordSource;
import com.nfsdb.ql.impl.virtual.VirtualColumnRecordSource;
import com.nfsdb.ql.ops.AddDoubleOperator;
import com.nfsdb.ql.ops.DoubleConstant;
import com.nfsdb.ql.ops.DoubleRecordSourceColumn;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class VirtualColumnTest extends AbstractTest {

    @Test
    public void testPlusDouble() throws Exception {

        final JournalWriter w = factory.writer(new JournalStructure("xyz") {{
            $str("ccy");
            $double("bid");
        }});

        Rnd rnd = new Rnd();

        for (int i = 0; i < 100; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, rnd.nextString(10));
            ew.putDouble(1, rnd.nextDouble());
            ew.append();
        }
        w.commit();

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        final AddDoubleOperator plus = (AddDoubleOperator) AddDoubleOperator.FACTORY.newInstance(null);
        plus.setName("plus");
        plus.setLhs(new DoubleRecordSourceColumn(w.getMetadata().getColumnIndex("bid")));
        plus.setRhs(new DoubleConstant(12.5));

        // select ccy, bid, bid+12.5 plus from xyz
        VirtualColumnRecordSource src = new VirtualColumnRecordSource(compiler.compileSource("xyz"), new ObjList<VirtualColumn>() {{
            add(plus);
        }});

        p.printCursor(src.prepareCursor(factory));

        final String expected = "VTJWCPSWHY\t-104.021850585938\t-91.521850585938\n" +
                "PEHNRXGZSX\t0.000020634160\t12.500020634160\n" +
                "IBBTGPGWFF\t0.000000567185\t12.500000567185\n" +
                "DEYYQEHBHF\t0.000000401164\t12.500000401164\n" +
                "LPDXYSBEOU\t384.072387695313\t396.572387695313\n" +
                "SHRUEDRQQU\t109.355468750000\t121.855468750000\n" +
                "FJGETJRSZS\t198.750000000000\t211.250000000000\n" +
                "RFBVTMHGOO\t172.796875000000\t185.296875000000\n" +
                "VDZJMYICCX\t-1024.000000000000\t-1011.500000000000\n" +
                "UICWEKGHVU\t0.000425009843\t12.500425009843\n" +
                "DOTSEDYYCT\t557.000000000000\t569.500000000000\n" +
                "OLYXWCKYLS\t0.000553289865\t12.500553289865\n" +
                "DSWUGSHOLN\t-1013.467773437500\t-1000.967773437500\n" +
                "IQBZXIOVIK\t512.000000000000\t524.500000000000\n" +
                "MSSUQSRLTK\t0.000000776007\t12.500000776007\n" +
                "SJOJIPHZEP\t174.774871826172\t187.274871826172\n" +
                "VLTOVLJUML\t-77.428833007813\t-64.928833007813\n" +
                "HMLLEOYPHR\t0.000327562877\t12.500327562877\n" +
                "ZIMNZZRMFM\t238.632812500000\t251.132812500000\n" +
                "ZGHWVDKFLO\t0.000102697388\t12.500102697388\n" +
                "OXPKRGIIHY\t128.000000000000\t140.500000000000\n" +
                "OQMYSSMPGL\t-144.421875000000\t-131.921875000000\n" +
                "HNZHZSQLDG\t832.000000000000\t844.500000000000\n" +
                "GIFOUSZMZV\t-200.000000000000\t-187.500000000000\n" +
                "BNDCQCEHNO\t2.602588653564\t15.102588653564\n" +
                "ELLKKHTWNW\t0.000969694171\t12.500969694171\n" +
                "FLRBROMNXK\t0.000000548919\t12.500000548919\n" +
                "ZULIGYVFZF\t-327.250000000000\t-314.750000000000\n" +
                "ZLUOGXHFVW\t0.000002116648\t12.500002116648\n" +
                "SRGOONFCLT\t310.101058959961\t322.601058959961\n" +
                "KFMQNTOGMX\t0.000012478828\t12.500012478828\n" +
                "LGMXSLUQDY\t0.000013214448\t12.500013214448\n" +
                "HNIMYFFDTN\t0.000000001910\t12.500000001910\n" +
                "FLPBNHGZWW\t695.173828125000\t707.673828125000\n" +
                "NGTNLEGPUH\t0.205350898206\t12.705350898206\n" +
                "UGGLNYRZLC\t638.000000000000\t650.500000000000\n" +
                "MIGQZVKHTL\t0.000000040298\t12.500000040298\n" +
                "SLQVFGPPRG\t-1024.000000000000\t-1011.500000000000\n" +
                "BHYSBQYMIZ\t5.336447119713\t17.836447119713\n" +
                "VTNPIWZNFK\t0.000000012570\t12.500000012570\n" +
                "VMCGFNWGRM\t-128.000000000000\t-115.500000000000\n" +
                "GIJYDVRVNG\t0.000000052204\t12.500000052204\n" +
                "EQODRZEIWF\t-31.000000000000\t-18.500000000000\n" +
                "KYHQQUWQOE\t880.000000000000\t892.500000000000\n" +
                "NEBQQEMXDK\t25.839271545410\t38.339271545410\n" +
                "JCTIZKYFLU\t768.000000000000\t780.500000000000\n" +
                "QSNPXMKJSM\t0.004184104619\t12.504184104619\n" +
                "XEYVTUPDHH\t845.823730468750\t858.323730468750\n" +
                "IWHPZRHHMG\t37.350353240967\t49.850353240967\n" +
                "YYFLSVIHDW\t-400.000000000000\t-387.500000000000\n" +
                "EVMLKCJBEV\t0.000000914462\t12.500000914462\n" +
                "HLIHYBTVZN\t0.000001439041\t12.500001439041\n" +
                "NXFSUWPNXH\t-199.648437500000\t-187.148437500000\n" +
                "TZODWKOCPF\t0.002563251997\t12.502563251997\n" +
                "PVKNCBWLNL\t0.397523656487\t12.897523656487\n" +
                "WQXYPOVFDB\t8.950848102570\t21.450848102570\n" +
                "NIJEEHRUGP\t448.000000000000\t460.500000000000\n" +
                "BTKVSBEGMI\t391.000000000000\t403.500000000000\n" +
                "NLKFNUHNRJ\t0.000000048669\t12.500000048669\n" +
                "BWVLOMPBET\t0.000038398017\t12.500038398017\n" +
                "KRIVOCUGPU\t2.929819107056\t15.429819107056\n" +
                "FIVQFNIZOS\t815.000000000000\t827.500000000000\n" +
                "SEPGIUQZHE\t0.017051883508\t12.517051883508\n" +
                "QHNOJIGFIN\t601.087127685547\t613.587127685547\n" +
                "QVZWEVQTQO\t-492.000000000000\t-479.500000000000\n" +
                "XTPNHTDCEB\t-966.000000000000\t-953.500000000000\n" +
                "XBBZVRLPTY\t70.810325622559\t83.310325622559\n" +
                "GYFUXCDKDW\t0.000837845349\t12.500837845349\n" +
                "DXCBJFRPXZ\t0.000008696692\t12.500008696692\n" +
                "XUNYQXTGNJ\t6.359375000000\t18.859375000000\n" +
                "LLEYMIWTCW\t792.242187500000\t804.742187500000\n" +
                "ORGFIEVMKP\t-726.000000000000\t-713.500000000000\n" +
                "GPYKKBMQMU\t0.000000000000\t12.500000000000\n" +
                "CIHCNPUGJO\t-762.240234375000\t-749.740234375000\n" +
                "EUKWMDNZZB\t0.000000031117\t12.500000031117\n" +
                "KOJSOLDYRO\t552.000000000000\t564.500000000000\n" +
                "PUNRPSMIFD\t73.947616577148\t86.447616577148\n" +
                "DKOEZBRQSQ\t-736.000000000000\t-723.500000000000\n" +
                "DIHHNSSTCR\t0.000002963134\t12.500002963134\n" +
                "PVQFULMERT\t0.000005817310\t12.500005817310\n" +
                "QBUYZVQQHS\t0.000000011688\t12.500000011688\n" +
                "PZPBHLNEJR\t-1003.625000000000\t-991.125000000000\n" +
                "IKDISGQFYQ\t351.509208679199\t364.009208679199\n" +
                "GPZNYVLTPK\t0.000702012621\t12.500702012621\n" +
                "QFNPOYNNCT\t0.000445737198\t12.500445737198\n" +
                "NSXHHDILEL\t0.001584486396\t12.501584486396\n" +
                "MMZSCJOUOU\t642.865234375000\t655.365234375000\n" +
                "ENFELWWRSL\t-934.268554687500\t-921.768554687500\n" +
                "QHGJBFQBBK\t103.908081054688\t116.408081054688\n" +
                "JZZYNPPBXB\t-944.000000000000\t-931.500000000000\n" +
                "RIIYMHOWKC\t0.000000542615\t12.500000542615\n" +
                "ZNLCNGZTOY\t0.000080315222\t12.500080315222\n" +
                "XRSFPVRQLG\t0.000000093226\t12.500000093226\n" +
                "ONNLITWGLF\t0.000000000000\t12.500000000000\n" +
                "QWPKLHTIIG\t-783.750000000000\t-771.250000000000\n" +
                "YYPDVRGRQG\t0.000376001219\t12.500376001219\n" +
                "PHKOWBVDEG\t0.000002666791\t12.500002666791\n" +
                "XGZMDJTHMH\t-670.500000000000\t-658.000000000000\n" +
                "VZHCNXZEQG\t-524.507812500000\t-512.007812500000\n" +
                "LUCFTLNKYT\t0.000000001835\t12.500000001835\n";

        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testSelectedColumns() throws Exception {

        final JournalWriter w = factory.writer(new JournalStructure("xyz") {{
            $str("ccy");
            $double("bid");
        }});

        Rnd rnd = new Rnd();

        for (int i = 0; i < 100; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, rnd.nextString(10));
            ew.putDouble(1, rnd.nextDouble());
            ew.append();
        }
        w.commit();

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        final AddDoubleOperator plus = (AddDoubleOperator) AddDoubleOperator.FACTORY.newInstance(null);
        plus.setName("plus");
        plus.setLhs(new DoubleRecordSourceColumn(w.getMetadata().getColumnIndex("bid")));
        plus.setRhs(new DoubleConstant(12.5));

        // select ccy, bid+12.5 plus from xyz
        RecordSource<? extends Record> src = new SelectedColumnsRecordSource(
                new VirtualColumnRecordSource(
                        compiler.compileSource("xyz"),
                        new ObjList<VirtualColumn>() {{
                            add(plus);
                        }}
                ),
                new ObjList<CharSequence>() {{
                    add("ccy");
                    add("plus");
                }});

        p.printCursor(src.prepareCursor(factory));

        final String expected = "VTJWCPSWHY\t-91.521850585938\n" +
                "PEHNRXGZSX\t12.500020634160\n" +
                "IBBTGPGWFF\t12.500000567185\n" +
                "DEYYQEHBHF\t12.500000401164\n" +
                "LPDXYSBEOU\t396.572387695313\n" +
                "SHRUEDRQQU\t121.855468750000\n" +
                "FJGETJRSZS\t211.250000000000\n" +
                "RFBVTMHGOO\t185.296875000000\n" +
                "VDZJMYICCX\t-1011.500000000000\n" +
                "UICWEKGHVU\t12.500425009843\n" +
                "DOTSEDYYCT\t569.500000000000\n" +
                "OLYXWCKYLS\t12.500553289865\n" +
                "DSWUGSHOLN\t-1000.967773437500\n" +
                "IQBZXIOVIK\t524.500000000000\n" +
                "MSSUQSRLTK\t12.500000776007\n" +
                "SJOJIPHZEP\t187.274871826172\n" +
                "VLTOVLJUML\t-64.928833007813\n" +
                "HMLLEOYPHR\t12.500327562877\n" +
                "ZIMNZZRMFM\t251.132812500000\n" +
                "ZGHWVDKFLO\t12.500102697388\n" +
                "OXPKRGIIHY\t140.500000000000\n" +
                "OQMYSSMPGL\t-131.921875000000\n" +
                "HNZHZSQLDG\t844.500000000000\n" +
                "GIFOUSZMZV\t-187.500000000000\n" +
                "BNDCQCEHNO\t15.102588653564\n" +
                "ELLKKHTWNW\t12.500969694171\n" +
                "FLRBROMNXK\t12.500000548919\n" +
                "ZULIGYVFZF\t-314.750000000000\n" +
                "ZLUOGXHFVW\t12.500002116648\n" +
                "SRGOONFCLT\t322.601058959961\n" +
                "KFMQNTOGMX\t12.500012478828\n" +
                "LGMXSLUQDY\t12.500013214448\n" +
                "HNIMYFFDTN\t12.500000001910\n" +
                "FLPBNHGZWW\t707.673828125000\n" +
                "NGTNLEGPUH\t12.705350898206\n" +
                "UGGLNYRZLC\t650.500000000000\n" +
                "MIGQZVKHTL\t12.500000040298\n" +
                "SLQVFGPPRG\t-1011.500000000000\n" +
                "BHYSBQYMIZ\t17.836447119713\n" +
                "VTNPIWZNFK\t12.500000012570\n" +
                "VMCGFNWGRM\t-115.500000000000\n" +
                "GIJYDVRVNG\t12.500000052204\n" +
                "EQODRZEIWF\t-18.500000000000\n" +
                "KYHQQUWQOE\t892.500000000000\n" +
                "NEBQQEMXDK\t38.339271545410\n" +
                "JCTIZKYFLU\t780.500000000000\n" +
                "QSNPXMKJSM\t12.504184104619\n" +
                "XEYVTUPDHH\t858.323730468750\n" +
                "IWHPZRHHMG\t49.850353240967\n" +
                "YYFLSVIHDW\t-387.500000000000\n" +
                "EVMLKCJBEV\t12.500000914462\n" +
                "HLIHYBTVZN\t12.500001439041\n" +
                "NXFSUWPNXH\t-187.148437500000\n" +
                "TZODWKOCPF\t12.502563251997\n" +
                "PVKNCBWLNL\t12.897523656487\n" +
                "WQXYPOVFDB\t21.450848102570\n" +
                "NIJEEHRUGP\t460.500000000000\n" +
                "BTKVSBEGMI\t403.500000000000\n" +
                "NLKFNUHNRJ\t12.500000048669\n" +
                "BWVLOMPBET\t12.500038398017\n" +
                "KRIVOCUGPU\t15.429819107056\n" +
                "FIVQFNIZOS\t827.500000000000\n" +
                "SEPGIUQZHE\t12.517051883508\n" +
                "QHNOJIGFIN\t613.587127685547\n" +
                "QVZWEVQTQO\t-479.500000000000\n" +
                "XTPNHTDCEB\t-953.500000000000\n" +
                "XBBZVRLPTY\t83.310325622559\n" +
                "GYFUXCDKDW\t12.500837845349\n" +
                "DXCBJFRPXZ\t12.500008696692\n" +
                "XUNYQXTGNJ\t18.859375000000\n" +
                "LLEYMIWTCW\t804.742187500000\n" +
                "ORGFIEVMKP\t-713.500000000000\n" +
                "GPYKKBMQMU\t12.500000000000\n" +
                "CIHCNPUGJO\t-749.740234375000\n" +
                "EUKWMDNZZB\t12.500000031117\n" +
                "KOJSOLDYRO\t564.500000000000\n" +
                "PUNRPSMIFD\t86.447616577148\n" +
                "DKOEZBRQSQ\t-723.500000000000\n" +
                "DIHHNSSTCR\t12.500002963134\n" +
                "PVQFULMERT\t12.500005817310\n" +
                "QBUYZVQQHS\t12.500000011688\n" +
                "PZPBHLNEJR\t-991.125000000000\n" +
                "IKDISGQFYQ\t364.009208679199\n" +
                "GPZNYVLTPK\t12.500702012621\n" +
                "QFNPOYNNCT\t12.500445737198\n" +
                "NSXHHDILEL\t12.501584486396\n" +
                "MMZSCJOUOU\t655.365234375000\n" +
                "ENFELWWRSL\t-921.768554687500\n" +
                "QHGJBFQBBK\t116.408081054688\n" +
                "JZZYNPPBXB\t-931.500000000000\n" +
                "RIIYMHOWKC\t12.500000542615\n" +
                "ZNLCNGZTOY\t12.500080315222\n" +
                "XRSFPVRQLG\t12.500000093226\n" +
                "ONNLITWGLF\t12.500000000000\n" +
                "QWPKLHTIIG\t-771.250000000000\n" +
                "YYPDVRGRQG\t12.500376001219\n" +
                "PHKOWBVDEG\t12.500002666791\n" +
                "XGZMDJTHMH\t-658.000000000000\n" +
                "VZHCNXZEQG\t-512.007812500000\n" +
                "LUCFTLNKYT\t12.500000001835\n";

        Assert.assertEquals(expected, sink.toString());
    }
}
