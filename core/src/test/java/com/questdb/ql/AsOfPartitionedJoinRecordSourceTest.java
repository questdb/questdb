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

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.join.AsOfJoinRecordSource;
import com.questdb.ql.join.AsOfPartitionedJoinRecordSource;
import com.questdb.ql.map.RecordKeyCopierCompiler;
import com.questdb.std.*;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class AsOfPartitionedJoinRecordSourceTest extends AbstractOptimiserTest {

    private static final CharSequenceHashSet keys = new CharSequenceHashSet();
    private static final RecordKeyCopierCompiler cc = new RecordKeyCopierCompiler(new BytecodeAssembler());


    @BeforeClass
    public static void setUpClass() throws Exception {

        FACTORY_CONTAINER.getFactory().getConfiguration().exists("");

        int xcount = 100;
        int ycount = 10;
        try (JournalWriter xw = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("x")
                .$ts()
                .$sym("ccy")
                .$double("rate")
                .$double("amount")
                .$str("trader")
                .$sym("contra")
                .$float("fl")
                .$short("sh")
                .$long("ln")
                .$bool("b")
                .recordCountHint(xcount)
                .$()
        )) {

            try (JournalWriter yw = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("y")
                    .$ts()
                    .$sym("ccy")
                    .$double("amount")
                    .$str("trader")
                    .recordCountHint(ycount)
                    .$()
            )) {

                Rnd rnd = new Rnd();

                String[] ccy = new String[3];
                for (int i = 0; i < ccy.length; i++) {
                    ccy[i] = rnd.nextChars(6).toString();
                }

                long ts = DateFormatUtils.parseDateTime("2015-03-10T00:00:00.000Z");

                for (int i = 0; i < xcount; i++) {
                    JournalEntryWriter w = xw.entryWriter();
                    w.putDate(0, ts += 10000);
                    w.putSym(1, ccy[rnd.nextPositiveInt() % ccy.length]);
                    w.putDouble(2, rnd.nextDouble());
                    w.putDouble(3, rnd.nextDouble());
                    if (rnd.nextBoolean()) {
                        w.putStr(4, rnd.nextChars(rnd.nextPositiveInt() % 128));
                    } else {
                        w.putNull(4);
                    }
                    w.putSym(5, ccy[rnd.nextPositiveInt() % ccy.length]);
                    w.putFloat(6, rnd.nextFloat());
                    w.putShort(7, (short) rnd.nextInt());
                    w.putLong(8, rnd.nextLong());
                    w.putBool(9, rnd.nextBoolean());
                    w.append();
                }
                xw.commit();

                ts = DateFormatUtils.parseDateTime("2015-03-10T00:00:00.000Z");
                for (int i = 0; i < ycount; i++) {
                    JournalEntryWriter w = yw.entryWriter();
                    w.putDate(0, ts += 60000);
                    w.putSym(1, ccy[rnd.nextPositiveInt() % ccy.length]);
                    w.putDouble(2, rnd.nextDouble());
                    if (rnd.nextBoolean()) {
                        w.putStr(3, rnd.nextChars(rnd.nextPositiveInt() % 128));
                    } else {
                        w.putNull(3);
                    }
                    w.append();
                }
                yw.commit();

                // records for adjacent join test

                try (JournalWriter jwa = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("a")
                        .$ts()
                        .$sym("ccy")
                        .$double("rate")
                        .$()
                )) {

                    try (JournalWriter jwb = FACTORY_CONTAINER.getFactory().writer(new JournalStructure("b")
                            .$ts()
                            .$sym("ccy")
                            .$double("amount")
                            .$()
                    )) {

                        JournalEntryWriter ewa;

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:30:00.000Z"));
                        ewa.putSym(1, "X");
                        ewa.putDouble(2, 0.538);
                        ewa.append();

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:35:00.000Z"));
                        ewa.putSym(1, "Y");
                        ewa.putDouble(2, 1.35);
                        ewa.append();

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:37:00.000Z"));
                        ewa.putSym(1, "Y");
                        ewa.putDouble(2, 1.41);
                        ewa.append();

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:39:00.000Z"));
                        ewa.putSym(1, "X");
                        ewa.putDouble(2, 0.601);
                        ewa.append();

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:40:00.000Z"));
                        ewa.putSym(1, "Y");
                        ewa.putDouble(2, 1.26);
                        ewa.append();

                        ewa = jwa.entryWriter();
                        ewa.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:43:00.000Z"));
                        ewa.putSym(1, "Y");
                        ewa.putDouble(2, 1.29);
                        ewa.append();

                        jwa.commit();

                        JournalEntryWriter ewb;

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:27:00.000Z"));
                        ewb.putSym(1, "X");
                        ewb.putDouble(2, 1100);
                        ewb.append();

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:28:00.000Z"));
                        ewb.putSym(1, "X");
                        ewb.putDouble(2, 1200);
                        ewb.append();

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:29:00.000Z"));
                        ewb.putSym(1, "X");
                        ewb.putDouble(2, 1500);
                        ewb.append();

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:34:50.000Z"));
                        ewb.putSym(1, "Y");
                        ewb.putDouble(2, 130);
                        ewb.append();

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:36:00.000Z"));
                        ewb.putSym(1, "Y");
                        ewb.putDouble(2, 150);
                        ewb.append();

                        ewb = jwb.entryWriter();
                        ewb.putDate(0, DateFormatUtils.parseDateTime("2014-03-12T10:41:00.000Z"));
                        ewb.putSym(1, "Y");
                        ewb.putDouble(2, 12000);
                        ewb.append();

                        jwb.commit();
                    }
                }
            }
        }
    }

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAdjacentRecordJoin() throws Exception {
        assertThat("timestamp\tccy\trate\ttimestamp\tccy\tamount\n" +
                        "2014-03-12T10:30:00.000Z\tX\t0.538000000000\t2014-03-12T10:29:00.000Z\tX\t1500.000000000000\n" +
                        "2014-03-12T10:35:00.000Z\tY\t1.350000000000\t2014-03-12T10:34:50.000Z\tY\t130.000000000000\n" +
                        "2014-03-12T10:37:00.000Z\tY\t1.410000000000\t2014-03-12T10:36:00.000Z\tY\t150.000000000000\n" +
                        "2014-03-12T10:39:00.000Z\tX\t0.601000000000\t\t\tNaN\n" +
                        "2014-03-12T10:40:00.000Z\tY\t1.260000000000\t\t\tNaN\n" +
                        "2014-03-12T10:43:00.000Z\tY\t1.290000000000\t2014-03-12T10:41:00.000Z\tY\t12000.000000000000\n",
                "a asof join b on a.ccy = b.ccy", true);
    }

    @Test
    public void testAmbiguousColumn() {
        try {
            expectFailure("select timestamp from y asof join x on x.ccy = y.ccy");
        } catch (ParserException e) {
            Assert.assertEquals(7, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Ambiguous"));
        }
    }

    @Test
    public void testAmbiguousColumnInFunc() {
        try {
            expectFailure("select sum(timestamp) from y asof join x on x.ccy = y.ccy");
        } catch (ParserException e) {
            Assert.assertEquals(11, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Ambiguous"));
        }
    }

    @Test
    public void testAnonymousSubqueriesFunc() {
        try {
            expectFailure("select sum(timestamp) from (y) asof join (x) on x.ccy = y.ccy");
        } catch (ParserException e) {
            Assert.assertEquals(48, QueryError.getPosition());
            TestUtils.assertEquals("Invalid journal name/alias", QueryError.getMessage());
        }
    }

    @Test
    public void testFixJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:40.000Z\tSWHYRX\t671.442138671875\t0.015470010694\tVTJWCP\t-7995393784734742820\t0.1341\t-20409\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:40.000Z\tVTJWCP\t386.843750000000\t1.195545256138\tVTJWCP\t1669226447966988582\t0.3845\t10793\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\tPEHNRX\t6436453824498875972\t0.4737\t21824\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tPEHNRX\t-3290351886406648039\t0.3296\t27881\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:03:40.000Z\tSWHYRX\t0.003415559302\t640.000000000000\tSWHYRX\t4092568845903588572\t0.5955\t15059\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tPEHNRX\t-8698821645604291033\t0.4353\t2237\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:30.000Z\tPEHNRX\t-519.289062500000\t0.082934856415\tVTJWCP\t595100009874933367\t0.0227\t-17193\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:20.000Z\tSWHYRX\t0.000009224671\t-642.406250000000\tSWHYRX\t7862014913865467812\t0.9481\t-22377\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:30.000Z\tPEHNRX\t0.033667386509\t64.000000000000\tPEHNRX\t6581120496001202966\t0.6449\t8754\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:40.000Z\tVTJWCP\t316.796386718750\t0.000000002997\tVTJWCP\t6490952371245756954\t0.4218\t-13242\tfalse\n";

        long memUsed = Unsafe.getMemUsed();
        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compileSource("y")
                , 0
                , new NoRowIdRecordSource().of(compileSource("select timestamp, ccy, rate, amount, contra, ln, fl, sh, b from x"))
                , 0
                , keys
                , keys
                , 128
                , 128
                , 128
                , cc
        )) {
            assertThat(expected, source);
        }
        Assert.assertEquals(memUsed, Unsafe.getMemUsed());
    }

    @Test
    public void testFixNonPartitionedJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:50.000Z\tPEHNRX\t0.299514681101\t768.000000000000\tSWHYRX\t-5710210982977201267\t0.0955\t-29572\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:50.000Z\tSWHYRX\t0.000036501544\t0.000000036384\tVTJWCP\t8810110521992874823\t0.5159\t28877\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\tPEHNRX\t6436453824498875972\t0.4737\t21824\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tPEHNRX\t-3290351886406648039\t0.3296\t27881\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:04:50.000Z\tPEHNRX\t70.810325622559\t0.000005221712\tSWHYRX\t6904166490726350488\t0.3863\t11305\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tPEHNRX\t-8698821645604291033\t0.4353\t2237\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:50.000Z\tVTJWCP\t-384.000000000000\t19.552153110504\tVTJWCP\t-3269323743905958237\t0.6822\t11402\ttrue\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:50.000Z\tPEHNRX\t272.870239257813\t-128.000000000000\tPEHNRX\t-5016390518489182614\t0.6254\t-8459\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:50.000Z\tVTJWCP\t0.004611001699\t0.000000023394\tVTJWCP\t8649805687735202371\t0.7444\t-20513\ttrue\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:50.000Z\tSWHYRX\t552.831069946289\t0.013248343952\tPEHNRX\t-9141139959474635253\t0.5255\t19898\ttrue\n";

        long memUsed = Unsafe.getMemUsed();
        try (AsOfJoinRecordSource source = new AsOfJoinRecordSource(
                compileSource("y")
                , 0
                , new NoRowIdRecordSource().of(compileSource("select timestamp, ccy, rate, amount, contra, ln, fl, sh, b from x"))
                , 0
        )) {
            assertThat(expected, source);
        }

        Assert.assertEquals(memUsed, Unsafe.getMemUsed());
    }

    @Test
    public void testNonPartitionedQuery() throws Exception {
        String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:50.000Z\tPEHNRX\t0.299514681101\t768.000000000000\t\tSWHYRX\t0.0955\t-29572\t-5710210982977201267\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:50.000Z\tSWHYRX\t0.000036501544\t0.000000036384\tHFVWSWSRGOO\tVTJWCP\t0.5159\t28877\t8810110521992874823\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:04:50.000Z\tPEHNRX\t70.810325622559\t0.000005221712\tXCDKDWOMDXCBJFRPXZSFXUNYQXTGNJJILLEYMIWTCWLFORGFIEVMKPYVGPYKKBMQMUDDCIHCNPUGJOPJEUKWMDNZZBBUKOJSOLDYRODIPUNRPSMIFDYPDK\tSWHYRX\t0.3863\t11305\t6904166490726350488\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:50.000Z\tVTJWCP\t-384.000000000000\t19.552153110504\tRHGKRKKUSIMYDXUUSKCXNMUREIJUHCLQCMZCCYVBDMQEHDHQHKSNGIZRPFM\tVTJWCP\t0.6822\t11402\t-3269323743905958237\ttrue\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:50.000Z\tPEHNRX\t272.870239257813\t-128.000000000000\t\tPEHNRX\t0.6254\t-8459\t-5016390518489182614\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:50.000Z\tVTJWCP\t0.004611001699\t0.000000023394\tUMKUBKXPMSXQSTVSTYSWHLSWPFHXDBXPNKGQELQDWQGMZBPHETSLOIMSUFXYIWE\tVTJWCP\t0.7444\t-20513\t8649805687735202371\ttrue\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:50.000Z\tSWHYRX\t552.831069946289\t0.013248343952\t\tPEHNRX\t0.5255\t19898\t-9141139959474635253\ttrue\n";
        assertThat(expected, "y asof join x");
    }

    @Test
    public void testPartitionedQuery() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:40.000Z\tSWHYRX\t671.442138671875\t0.015470010694\t\tVTJWCP\t0.1341\t-20409\t-7995393784734742820\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:40.000Z\tVTJWCP\t386.843750000000\t1.195545256138\t\tVTJWCP\t0.3845\t10793\t1669226447966988582\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:03:40.000Z\tSWHYRX\t0.003415559302\t640.000000000000\t\tSWHYRX\t0.5955\t15059\t4092568845903588572\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:30.000Z\tPEHNRX\t-519.289062500000\t0.082934856415\t\tVTJWCP\t0.0227\t-17193\t595100009874933367\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:20.000Z\tSWHYRX\t0.000009224671\t-642.406250000000\tIEBSQCNSFFLTRYZUZYJIHZBWWXFQDCQSC\tSWHYRX\t0.9481\t-22377\t7862014913865467812\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:30.000Z\tPEHNRX\t0.033667386509\t64.000000000000\tDSWXYYYVSYYEQBORDTQHVCVUYGMBMKSCPWLZKDMPVRHW\tPEHNRX\t0.6449\t8754\t6581120496001202966\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:40.000Z\tVTJWCP\t316.796386718750\t0.000000002997\tMCIYIXGHRQQTKOJEDNKRCGKSQDCMUMKNJGSPETBBQDSRDJWIMGPLRQUJJFGQIZKMDCXYTRVYQNFPGSQIXMIIFSWXSDDJSPKSHRIH\tVTJWCP\t0.4218\t-13242\t6490952371245756954\tfalse\n";
        assertThat(expected, "y asof join x on x.ccy = y.ccy");
    }

    @Test
    public void testPartitionedQuerySimplifiedJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:40.000Z\tSWHYRX\t671.442138671875\t0.015470010694\t\tVTJWCP\t0.1341\t-20409\t-7995393784734742820\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:40.000Z\tVTJWCP\t386.843750000000\t1.195545256138\t\tVTJWCP\t0.3845\t10793\t1669226447966988582\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:03:40.000Z\tSWHYRX\t0.003415559302\t640.000000000000\t\tSWHYRX\t0.5955\t15059\t4092568845903588572\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:30.000Z\tPEHNRX\t-519.289062500000\t0.082934856415\t\tVTJWCP\t0.0227\t-17193\t595100009874933367\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:20.000Z\tSWHYRX\t0.000009224671\t-642.406250000000\tIEBSQCNSFFLTRYZUZYJIHZBWWXFQDCQSC\tSWHYRX\t0.9481\t-22377\t7862014913865467812\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:30.000Z\tPEHNRX\t0.033667386509\t64.000000000000\tDSWXYYYVSYYEQBORDTQHVCVUYGMBMKSCPWLZKDMPVRHW\tPEHNRX\t0.6449\t8754\t6581120496001202966\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:40.000Z\tVTJWCP\t316.796386718750\t0.000000002997\tMCIYIXGHRQQTKOJEDNKRCGKSQDCMUMKNJGSPETBBQDSRDJWIMGPLRQUJJFGQIZKMDCXYTRVYQNFPGSQIXMIIFSWXSDDJSPKSHRIH\tVTJWCP\t0.4218\t-13242\t6490952371245756954\tfalse\n";
        assertThat(expected, "y asof join x on (ccy)");
    }

    @Test
    public void testRowidJoin() throws Exception {
        final String expected = "timestamp\tccy\tamount\ttrader\ttimestamp\tccy\trate\tamount\ttrader\tcontra\tfl\tsh\tln\tb\n" +
                "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:40.000Z\tSWHYRX\t671.442138671875\t0.015470010694\t\tVTJWCP\t0.1341\t-20409\t-7995393784734742820\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:40.000Z\tVTJWCP\t386.843750000000\t1.195545256138\t\tVTJWCP\t0.3845\t10793\t1669226447966988582\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:03:40.000Z\tSWHYRX\t0.003415559302\t640.000000000000\t\tSWHYRX\t0.5955\t15059\t4092568845903588572\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:30.000Z\tPEHNRX\t-519.289062500000\t0.082934856415\t\tVTJWCP\t0.0227\t-17193\t595100009874933367\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:20.000Z\tSWHYRX\t0.000009224671\t-642.406250000000\tIEBSQCNSFFLTRYZUZYJIHZBWWXFQDCQSC\tSWHYRX\t0.9481\t-22377\t7862014913865467812\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:30.000Z\tPEHNRX\t0.033667386509\t64.000000000000\tDSWXYYYVSYYEQBORDTQHVCVUYGMBMKSCPWLZKDMPVRHW\tPEHNRX\t0.6449\t8754\t6581120496001202966\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:40.000Z\tVTJWCP\t316.796386718750\t0.000000002997\tMCIYIXGHRQQTKOJEDNKRCGKSQDCMUMKNJGSPETBBQDSRDJWIMGPLRQUJJFGQIZKMDCXYTRVYQNFPGSQIXMIIFSWXSDDJSPKSHRIH\tVTJWCP\t0.4218\t-13242\t6490952371245756954\tfalse\n";

        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compileSource("y")
                , 0
                , compileSource("x")
                , 0
                , keys
                , keys
                , 512
                , 512
                , 512
                , cc
        )) {
            assertThat(expected, source, true);
        }
    }

    @Test
    public void testRowidNonPartitioned() throws Exception {

        AsOfJoinRecordSource source = new AsOfJoinRecordSource(
                compileSource("y")
                , 0
                , compileSource("x")
                , 0
        );

        String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:50.000Z\tPEHNRX\t0.299514681101\t768.000000000000\t\tSWHYRX\t0.0955\t-29572\t-5710210982977201267\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:50.000Z\tSWHYRX\t0.000036501544\t0.000000036384\tHFVWSWSRGOO\tVTJWCP\t0.5159\t28877\t8810110521992874823\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:04:50.000Z\tPEHNRX\t70.810325622559\t0.000005221712\tXCDKDWOMDXCBJFRPXZSFXUNYQXTGNJJILLEYMIWTCWLFORGFIEVMKPYVGPYKKBMQMUDDCIHCNPUGJOPJEUKWMDNZZBBUKOJSOLDYRODIPUNRPSMIFDYPDK\tSWHYRX\t0.3863\t11305\t6904166490726350488\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:50.000Z\tVTJWCP\t-384.000000000000\t19.552153110504\tRHGKRKKUSIMYDXUUSKCXNMUREIJUHCLQCMZCCYVBDMQEHDHQHKSNGIZRPFM\tVTJWCP\t0.6822\t11402\t-3269323743905958237\ttrue\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:50.000Z\tPEHNRX\t272.870239257813\t-128.000000000000\t\tPEHNRX\t0.6254\t-8459\t-5016390518489182614\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:50.000Z\tVTJWCP\t0.004611001699\t0.000000023394\tUMKUBKXPMSXQSTVSTYSWHLSWPFHXDBXPNKGQELQDWQGMZBPHETSLOIMSUFXYIWE\tVTJWCP\t0.7444\t-20513\t8649805687735202371\ttrue\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:50.000Z\tSWHYRX\t552.831069946289\t0.013248343952\t\tPEHNRX\t0.5255\t19898\t-9141139959474635253\ttrue\n";
        assertThat(expected, source);
    }

    @Test
    public void testStrings() throws Exception {
        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compileSource("y")
                , 0
                , new NoRowIdRecordSource().of(compileSource("x"))
                , 0
                , keys
                , keys
                , 512
                , 512
                , 512
                , cc
        )) {
            StringSink testSink = new StringSink();
            int idx = source.getMetadata().getColumnIndex("trader");
            RecordCursor cursor = source.prepareCursor(FACTORY_CONTAINER.getFactory());
            try {
                for (Record r : cursor) {
                    testSink.clear();
                    r.getStr(idx, testSink);

                    if (r.getFlyweightStr(idx) == null) {
                        Assert.assertTrue(testSink.length() == 0);
                    } else {
                        TestUtils.assertEquals(r.getFlyweightStr(idx), testSink);
                    }
                    TestUtils.assertEquals(r.getFlyweightStr(idx), r.getFlyweightStr(idx));
                }
            } finally {
                cursor.releaseCursor();
            }
        }
    }

    @Test
    public void testVarJoin() throws Exception {
        final String expected = "timestamp\tccy\tamount\ttrader\ttimestamp\tccy\trate\tamount\ttrader\tcontra\tfl\tsh\tln\tb\n" +
                "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:40.000Z\tSWHYRX\t671.442138671875\t0.015470010694\t\tVTJWCP\t0.1341\t-20409\t-7995393784734742820\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:40.000Z\tVTJWCP\t386.843750000000\t1.195545256138\t\tVTJWCP\t0.3845\t10793\t1669226447966988582\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:03:40.000Z\tSWHYRX\t0.003415559302\t640.000000000000\t\tSWHYRX\t0.5955\t15059\t4092568845903588572\tfalse\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:30.000Z\tPEHNRX\t-519.289062500000\t0.082934856415\t\tVTJWCP\t0.0227\t-17193\t595100009874933367\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:20.000Z\tSWHYRX\t0.000009224671\t-642.406250000000\tIEBSQCNSFFLTRYZUZYJIHZBWWXFQDCQSC\tSWHYRX\t0.9481\t-22377\t7862014913865467812\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:30.000Z\tPEHNRX\t0.033667386509\t64.000000000000\tDSWXYYYVSYYEQBORDTQHVCVUYGMBMKSCPWLZKDMPVRHW\tPEHNRX\t0.6449\t8754\t6581120496001202966\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:40.000Z\tVTJWCP\t316.796386718750\t0.000000002997\tMCIYIXGHRQQTKOJEDNKRCGKSQDCMUMKNJGSPETBBQDSRDJWIMGPLRQUJJFGQIZKMDCXYTRVYQNFPGSQIXMIIFSWXSDDJSPKSHRIH\tVTJWCP\t0.4218\t-13242\t6490952371245756954\tfalse\n";

        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compileSource("y")
                , 0
                , new NoRowIdRecordSource().of(compileSource("x"))
                , 0
                , keys
                , keys
                , 512
                , 512
                , 512
                , cc
        )) {
            assertThat(expected, source, true);
        }
    }

    @Test
    public void testVarNonPartitioned() throws Exception {

        try (AsOfJoinRecordSource source = new AsOfJoinRecordSource(
                compileSource("y")
                , 0
                , new NoRowIdRecordSource().of(compileSource("x"))
                , 0
        )) {

            String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t9.986581325531\tDHM\t2015-03-10T00:00:50.000Z\tPEHNRX\t0.299514681101\t768.000000000000\t\tSWHYRX\t0.0955\t-29572\t-5710210982977201267\ttrue\n" +
                    "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000022642\tHBEKCGJOZWRXKMTFXRYPHFPUYWNLBVVHN\t2015-03-10T00:01:50.000Z\tSWHYRX\t0.000036501544\t0.000000036384\tHFVWSWSRGOO\tVTJWCP\t0.5159\t28877\t8810110521992874823\tfalse\n" +
                    "2015-03-10T00:03:00.000Z\tVTJWCP\t0.000000012344\tRTLXHBHDHIMFYOJREFUTMSGUYXLXWLUKSXSCMGFCDFGVDKHCZIUISSCBVCLYMFERSXQCHTKLTNYILMDTHTXDEHNVMEVIJQRJMLJKFYHZXH\t2015-03-10T00:02:50.000Z\tVTJWCP\t-353.683593750000\t0.000000000000\t\tPEHNRX\t0.4737\t21824\t6436453824498875972\tfalse\n" +
                    "2015-03-10T00:04:00.000Z\tPEHNRX\t0.000000006259\tGOVGNCFYDU\t2015-03-10T00:03:50.000Z\tPEHNRX\t25.839271545410\t0.360216885805\tZKYFLUHZQSNPXMKJSMKIXEYVTUPDHHGGIWHPZRHHMGZJYYFLSVIHDWWLEVMLKCJBEVLUHLIHYBTVZNCLN\tPEHNRX\t0.3296\t27881\t-3290351886406648039\tfalse\n" +
                    "2015-03-10T00:05:00.000Z\tSWHYRX\t-1024.000000000000\tZJBFLWWXEBZTZYTHPWGBNPIIFNYPCBTIOJYWUIYFQPXWVETMPCONIJMVFQFDBOMQBLBVQHLSYJUEGYZYOOMNSZVWS\t2015-03-10T00:04:50.000Z\tPEHNRX\t70.810325622559\t0.000005221712\tXCDKDWOMDXCBJFRPXZSFXUNYQXTGNJJILLEYMIWTCWLFORGFIEVMKPYVGPYKKBMQMUDDCIHCNPUGJOPJEUKWMDNZZBBUKOJSOLDYRODIPUNRPSMIFDYPDK\tSWHYRX\t0.3863\t11305\t6904166490726350488\tfalse\n" +
                    "2015-03-10T00:06:00.000Z\tVTJWCP\t800.000000000000\tEBNYHKWBXMYTZSUXQSWVRVUOSTZQBMERYZ\t2015-03-10T00:05:50.000Z\tVTJWCP\t12.456869840622\t55.575583457947\tDILELRUMMZSCJOUOUIGENFELWWRSLBMQHGJBFQBBKFIJZZYNPPB\tPEHNRX\t0.4353\t2237\t-8698821645604291033\tfalse\n" +
                    "2015-03-10T00:07:00.000Z\tPEHNRX\t0.000000057413\t\t2015-03-10T00:06:50.000Z\tVTJWCP\t-384.000000000000\t19.552153110504\tRHGKRKKUSIMYDXUUSKCXNMUREIJUHCLQCMZCCYVBDMQEHDHQHKSNGIZRPFM\tVTJWCP\t0.6822\t11402\t-3269323743905958237\ttrue\n" +
                    "2015-03-10T00:08:00.000Z\tSWHYRX\t0.897577673197\t\t2015-03-10T00:07:50.000Z\tPEHNRX\t272.870239257813\t-128.000000000000\t\tPEHNRX\t0.6254\t-8459\t-5016390518489182614\tfalse\n" +
                    "2015-03-10T00:09:00.000Z\tPEHNRX\t797.375000000000\tKXQHOKXHXYWTYFMYVYBVUBHMYQRVVMKMIPOVRTZDGOG\t2015-03-10T00:08:50.000Z\tVTJWCP\t0.004611001699\t0.000000023394\tUMKUBKXPMSXQSTVSTYSWHLSWPFHXDBXPNKGQELQDWQGMZBPHETSLOIMSUFXYIWE\tVTJWCP\t0.7444\t-20513\t8649805687735202371\ttrue\n" +
                    "2015-03-10T00:10:00.000Z\tVTJWCP\t0.000456069203\t\t2015-03-10T00:09:50.000Z\tSWHYRX\t552.831069946289\t0.013248343952\t\tPEHNRX\t0.5255\t19898\t-9141139959474635253\ttrue\n";
            printer.print(source, FACTORY_CONTAINER.getFactory());
            TestUtils.assertEquals(expected, sink);
        }
    }

    private void assertThat(String expected, RecordSource source) throws IOException {
        assertThat(expected, source, false);
    }

    static {
        keys.add("ccy");
    }
}
