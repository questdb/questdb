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
import com.nfsdb.collections.CharSequenceHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.impl.AsOfPartitionedJoinRecordSource;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Rnd;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsOfPartitionedJoinRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        JournalWriter jw = factory.writer(new JournalStructure("x")
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
                        .$()
        );

        JournalWriter jwy = factory.writer(new JournalStructure("y")
                        .$ts()
                        .$sym("ccy")
                        .$double("amount")
                        .$str("trader")
                        .$()
        );

        Rnd rnd = new Rnd();

        String[] ccy = new String[3];
        for (int i = 0; i < ccy.length; i++) {
            ccy[i] = rnd.nextChars(6).toString();
        }

        int count = 100;
        long ts = Dates.parseDateTime("2015-03-10T00:00:00.000Z");

        for (int i = 0; i < count; i++) {
            JournalEntryWriter w = jw.entryWriter();
            w.putDate(0, ts += 10000);
            w.putSym(1, ccy[rnd.nextPositiveInt() % ccy.length]);
            w.putDouble(2, rnd.nextDouble());
            w.putDouble(3, rnd.nextDouble());
            w.putStr(4, rnd.nextChars(rnd.nextPositiveInt() % 128));
            w.putSym(5, ccy[rnd.nextPositiveInt() % ccy.length]);
            w.putFloat(6, rnd.nextFloat());
            w.putShort(7, (short) rnd.nextInt());
            w.putLong(8, rnd.nextLong());
            w.putBool(9, rnd.nextBoolean());
            w.append();
        }
        jw.commit();

        int county = 10;
        ts = Dates.parseDateTime("2015-03-10T00:00:00.000Z");
        for (int i = 0; i < county; i++) {
            JournalEntryWriter w = jwy.entryWriter();
            w.putDate(0, ts += 60000);
            w.putSym(1, ccy[rnd.nextPositiveInt() % ccy.length]);
            w.putDouble(2, rnd.nextDouble());
            w.putStr(3, rnd.nextChars(rnd.nextPositiveInt() % 128));
            w.append();
        }
        jwy.commit();
    }

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testFixJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t0.937527447939\tIYMQGYIYHVZMXGRFXUIUNMOQUIHPNGNOTXDHUZFW\t2015-03-10T00:00:50.000Z\t0.000039573626\t0.000003805120\tVTJWCP\t-5106801657083469087\t0.2093\t-20638\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tSWHYRX\t-354.250000000000\tREQIELGOYUKUTNWDLEXTVTXMGNRSVIVWEDZMVQTSYCVPGQMEYLBGSLMIBQLXNLKYSPOEXUVJHZQ\t2015-03-10T00:01:50.000Z\t832.000000000000\t0.759080171585\tSWHYRX\t-6913510864836958686\t0.2185\t-24061\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.016129214317\tQBMDSVCBRNNDKHPDGPEGWYXIVMNRTOYZSBBJSQBCEIBVNGVPPMOEQHHTNCWVRYTTYNRSW\t2015-03-10T00:02:30.000Z\t0.000005960636\t0.000000006302\tSWHYRX\t-6595197632099589183\t0.4355\t24525\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tSWHYRX\t-502.603027343750\tPRIWBBOOYOBEXRYNHRGGBDEWWROZTQQDOGUVJHQJHNYWCXWTBBMMDBBHLPGXIIDYSTGXRGUOXFHBLMYFVFFOB\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tVTJWCP\t-1359049242368089934\t0.4722\t26075\ttrue\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t0.219250522554\tQYDQVLYIWPQGNVZWJRSVPJMLMGICUWCLPILEQDWUEGKNHVIUZWTOUVQSBYFQNNEJHTUTCFEZMFZKNEONSLDSLQSLNVTKIGKFBSFCIGYPWDWVTRWXECKLLNKJGMGF\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tVTJWCP\t-1359049242368089934\t0.4722\t26075\ttrue\n" +
                "2015-03-10T00:06:00.000Z\tSWHYRX\t0.000029225861\tZRYSTR\t2015-03-10T00:05:20.000Z\t163.814239501953\t214.940444946289\tSWHYRX\t-6991567553287980963\t0.6683\t-14466\ttrue\n" +
                "2015-03-10T00:07:00.000Z\tVTJWCP\t433.343750000000\tMYJGIFYQXXYMGDPKZEXYHDHKKOJNOXBRMQMPZDVYQBBWZVLJYFXSBNVNGPNLNJZLD\t2015-03-10T00:06:10.000Z\t-168.712890625000\t0.000002090942\tVTJWCP\t7827920822553960170\t0.7780\t-15452\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t-810.375000000000\tPULKHMJLLKQZJIONCLBYNYYWYBEPKPNZXNYWIGPCMLCBMUPYMRIGQWSZMUMXMSYXCEEDCL\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tPEHNRX\t3041632938449863492\t0.4069\t13732\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tSWHYRX\t-384.000000000000\tZGUJBKNTPYXUBYXGDDULXVVSCNJINCQSDOQILSLXZEMDBLNXHYUUTVSXURFLRJLIUC\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tPEHNRX\t3041632938449863492\t0.4069\t13732\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t384.000000000000\tPGKJRQGKHQHXYUVDUZQTICMPWFZEINPQOGHUGZGDCFLNGCEFBTDNSYQTIGUTKIESOSYYLIBUFGPWTQJQWTGERXRSYZCKPFWECEH\t2015-03-10T00:09:50.000Z\t0.062803771347\t896.000000000000\tPEHNRX\t-5743731661904518905\t0.9202\t-15664\ttrue\n";

        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compiler.compileSource("y")
                , 0
                , new NoRowidSource().of(compiler.compileSource("select timestamp, ccy, rate, amount, contra, ln, fl, sh, b from x"))
                , 0
                ,
                new CharSequenceHashSet() {{
                    add("ccy");
                }}
                , 128
        )) {
            printer.printCursor(source.prepareCursor(factory));
            TestUtils.assertEquals(expected, sink);
        }
    }

    @Test
    public void testRowidJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t0.937527447939\tIYMQGYIYHVZMXGRFXUIUNMOQUIHPNGNOTXDHUZFW\t2015-03-10T00:00:50.000Z\t0.000039573626\t0.000003805120\tSRGOONFCLTJCKFMQNTOGMXUKLGMXSLUQDYOPHNIMYFFDTNPHFLPBNHGZWWCCNGTNLEGPUHHIUGGLNYRZLCBDMIGQZVKHTLQZ\tVTJWCP\t0.2093\t-20638\t-5106801657083469087\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tSWHYRX\t-354.250000000000\tREQIELGOYUKUTNWDLEXTVTXMGNRSVIVWEDZMVQTSYCVPGQMEYLBGSLMIBQLXNLKYSPOEXUVJHZQ\t2015-03-10T00:01:50.000Z\t832.000000000000\t0.759080171585\tEYMIWTCWLFORGFIEVMKPYVGPYKKBMQMUDDCIHCNPUGJOPJEUKWMDNZZBBUKOJSOLDYRODIPUNRPSMIFDYPDKOEZBRQSQJGDIHHNSSTCRZUPVQFULMERTPIQ\tSWHYRX\t0.2185\t-24061\t-6913510864836958686\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.016129214317\tQBMDSVCBRNNDKHPDGPEGWYXIVMNRTOYZSBBJSQBCEIBVNGVPPMOEQHHTNCWVRYTTYNRSW\t2015-03-10T00:02:30.000Z\t0.000005960636\t0.000000006302\tKMEKPFOYMNWDSWLUVDRHFBCZIOLYLPGZHITQJLKTRDLVSYLMSRHGKRKKUSIMYDXUUSKCXNMUREIJUHCLQCMZCCYVBDMQEHDHQHKSNGIZRPFMDVVGSVCLLERSMK\tSWHYRX\t0.4355\t24525\t-6595197632099589183\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tSWHYRX\t-502.603027343750\tPRIWBBOOYOBEXRYNHRGGBDEWWROZTQQDOGUVJHQJHNYWCXWTBBMMDBBHLPGXIIDYSTGXRGUOXFHBLMYFVFFOB\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tTFBYHSHBXOWVYUVV\tVTJWCP\t0.4722\t26075\t-1359049242368089934\ttrue\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t0.219250522554\tQYDQVLYIWPQGNVZWJRSVPJMLMGICUWCLPILEQDWUEGKNHVIUZWTOUVQSBYFQNNEJHTUTCFEZMFZKNEONSLDSLQSLNVTKIGKFBSFCIGYPWDWVTRWXECKLLNKJGMGF\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tTFBYHSHBXOWVYUVV\tVTJWCP\t0.4722\t26075\t-1359049242368089934\ttrue\n" +
                "2015-03-10T00:06:00.000Z\tSWHYRX\t0.000029225861\tZRYSTR\t2015-03-10T00:05:20.000Z\t163.814239501953\t214.940444946289\tSQIIQQLRUOELSRCPUVJNSLVBETOPFWMSRNFKFZJKOJRBGMXCVFWUFLIUPNYDPZLIVLYZUBJTWBUHZSPTTXEZMFYLBVBDTCLGEJBYBSJ\tSWHYRX\t0.6683\t-14466\t-6991567553287980963\ttrue\n" +
                "2015-03-10T00:07:00.000Z\tVTJWCP\t433.343750000000\tMYJGIFYQXXYMGDPKZEXYHDHKKOJNOXBRMQMPZDVYQBBWZVLJYFXSBNVNGPNLNJZLD\t2015-03-10T00:06:10.000Z\t-168.712890625000\t0.000002090942\tFDRCUHNDUDQXFKEMPPXOCYFWMEZBPNNMZYULBZKXPTEFQGNXLFIUPZTUP\tVTJWCP\t0.7780\t-15452\t7827920822553960170\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t-810.375000000000\tPULKHMJLLKQZJIONCLBYNYYWYBEPKPNZXNYWIGPCMLCBMUPYMRIGQWSZMUMXMSYXCEEDCL\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tYOPOQHKIZCCIQFUQYLJKPTDPZFOMEFUVYSMIYXIPGTDBCYCEJFPBYNORYJVMWNFXMVWRODBYSMBTZISISRZBSRBOXYTQXNZKTVOPKBXOHXYMMIFMMSWIBSLSVJ\tPEHNRX\t0.4069\t13732\t3041632938449863492\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tSWHYRX\t-384.000000000000\tZGUJBKNTPYXUBYXGDDULXVVSCNJINCQSDOQILSLXZEMDBLNXHYUUTVSXURFLRJLIUC\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tYOPOQHKIZCCIQFUQYLJKPTDPZFOMEFUVYSMIYXIPGTDBCYCEJFPBYNORYJVMWNFXMVWRODBYSMBTZISISRZBSRBOXYTQXNZKTVOPKBXOHXYMMIFMMSWIBSLSVJ\tPEHNRX\t0.4069\t13732\t3041632938449863492\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t384.000000000000\tPGKJRQGKHQHXYUVDUZQTICMPWFZEINPQOGHUGZGDCFLNGCEFBTDNSYQTIGUTKIESOSYYLIBUFGPWTQJQWTGERXRSYZCKPFWECEH\t2015-03-10T00:09:50.000Z\t0.062803771347\t896.000000000000\tYVJISIQFNSEUHOSVSIKJFJLNEKTSLZFPGDVCLMZTXOYEPKECCJZJOSDCIWCZECJGNWQNKCYVZJRRZYDBL\tPEHNRX\t0.9202\t-15664\t-5743731661904518905\ttrue\n";

        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compiler.compileSource("y")
                , 0
                , compiler.compileSource("x")
                , 0
                ,
                new CharSequenceHashSet() {{
                    add("ccy");
                }}
                , 512
        )) {
            printer.printCursor(source.prepareCursor(factory));
            TestUtils.assertEquals(expected, sink);
        }
    }

    @Test
    public void testStrings() throws Exception {
        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compiler.compileSource("y")
                , 0
                , new NoRowidSource().of(compiler.compileSource("x"))
                , 0
                ,
                new CharSequenceHashSet() {{
                    add("ccy");
                }}
                , 512
        )) {
            StringSink testSink = new StringSink();
            int idx = source.getMetadata().getColumnIndex("trader");
            for (Record r : source.prepareCursor(factory)) {
                testSink.clear();
                r.getStr(idx, testSink);
                TestUtils.assertEquals(r.getStr(idx), testSink);
                TestUtils.assertEquals(r.getStr(idx), r.getFlyweightStr(idx));
            }
        }
    }

    @Test
    public void testVarJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t0.937527447939\tIYMQGYIYHVZMXGRFXUIUNMOQUIHPNGNOTXDHUZFW\t2015-03-10T00:00:50.000Z\t0.000039573626\t0.000003805120\tSRGOONFCLTJCKFMQNTOGMXUKLGMXSLUQDYOPHNIMYFFDTNPHFLPBNHGZWWCCNGTNLEGPUHHIUGGLNYRZLCBDMIGQZVKHTLQZ\tVTJWCP\t0.2093\t-20638\t-5106801657083469087\ttrue\n" +
                "2015-03-10T00:02:00.000Z\tSWHYRX\t-354.250000000000\tREQIELGOYUKUTNWDLEXTVTXMGNRSVIVWEDZMVQTSYCVPGQMEYLBGSLMIBQLXNLKYSPOEXUVJHZQ\t2015-03-10T00:01:50.000Z\t832.000000000000\t0.759080171585\tEYMIWTCWLFORGFIEVMKPYVGPYKKBMQMUDDCIHCNPUGJOPJEUKWMDNZZBBUKOJSOLDYRODIPUNRPSMIFDYPDKOEZBRQSQJGDIHHNSSTCRZUPVQFULMERTPIQ\tSWHYRX\t0.2185\t-24061\t-6913510864836958686\tfalse\n" +
                "2015-03-10T00:03:00.000Z\tVTJWCP\t0.016129214317\tQBMDSVCBRNNDKHPDGPEGWYXIVMNRTOYZSBBJSQBCEIBVNGVPPMOEQHHTNCWVRYTTYNRSW\t2015-03-10T00:02:30.000Z\t0.000005960636\t0.000000006302\tKMEKPFOYMNWDSWLUVDRHFBCZIOLYLPGZHITQJLKTRDLVSYLMSRHGKRKKUSIMYDXUUSKCXNMUREIJUHCLQCMZCCYVBDMQEHDHQHKSNGIZRPFMDVVGSVCLLERSMK\tSWHYRX\t0.4355\t24525\t-6595197632099589183\tfalse\n" +
                "2015-03-10T00:04:00.000Z\tSWHYRX\t-502.603027343750\tPRIWBBOOYOBEXRYNHRGGBDEWWROZTQQDOGUVJHQJHNYWCXWTBBMMDBBHLPGXIIDYSTGXRGUOXFHBLMYFVFFOB\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tTFBYHSHBXOWVYUVV\tVTJWCP\t0.4722\t26075\t-1359049242368089934\ttrue\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t0.219250522554\tQYDQVLYIWPQGNVZWJRSVPJMLMGICUWCLPILEQDWUEGKNHVIUZWTOUVQSBYFQNNEJHTUTCFEZMFZKNEONSLDSLQSLNVTKIGKFBSFCIGYPWDWVTRWXECKLLNKJGMGF\t2015-03-10T00:03:40.000Z\t0.000355324199\t-602.687500000000\tTFBYHSHBXOWVYUVV\tVTJWCP\t0.4722\t26075\t-1359049242368089934\ttrue\n" +
                "2015-03-10T00:06:00.000Z\tSWHYRX\t0.000029225861\tZRYSTR\t2015-03-10T00:05:20.000Z\t163.814239501953\t214.940444946289\tSQIIQQLRUOELSRCPUVJNSLVBETOPFWMSRNFKFZJKOJRBGMXCVFWUFLIUPNYDPZLIVLYZUBJTWBUHZSPTTXEZMFYLBVBDTCLGEJBYBSJ\tSWHYRX\t0.6683\t-14466\t-6991567553287980963\ttrue\n" +
                "2015-03-10T00:07:00.000Z\tVTJWCP\t433.343750000000\tMYJGIFYQXXYMGDPKZEXYHDHKKOJNOXBRMQMPZDVYQBBWZVLJYFXSBNVNGPNLNJZLD\t2015-03-10T00:06:10.000Z\t-168.712890625000\t0.000002090942\tFDRCUHNDUDQXFKEMPPXOCYFWMEZBPNNMZYULBZKXPTEFQGNXLFIUPZTUP\tVTJWCP\t0.7780\t-15452\t7827920822553960170\tfalse\n" +
                "2015-03-10T00:08:00.000Z\tSWHYRX\t-810.375000000000\tPULKHMJLLKQZJIONCLBYNYYWYBEPKPNZXNYWIGPCMLCBMUPYMRIGQWSZMUMXMSYXCEEDCL\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tYOPOQHKIZCCIQFUQYLJKPTDPZFOMEFUVYSMIYXIPGTDBCYCEJFPBYNORYJVMWNFXMVWRODBYSMBTZISISRZBSRBOXYTQXNZKTVOPKBXOHXYMMIFMMSWIBSLSVJ\tPEHNRX\t0.4069\t13732\t3041632938449863492\tfalse\n" +
                "2015-03-10T00:09:00.000Z\tSWHYRX\t-384.000000000000\tZGUJBKNTPYXUBYXGDDULXVVSCNJINCQSDOQILSLXZEMDBLNXHYUUTVSXURFLRJLIUC\t2015-03-10T00:07:30.000Z\t28.844047546387\t329.886169433594\tYOPOQHKIZCCIQFUQYLJKPTDPZFOMEFUVYSMIYXIPGTDBCYCEJFPBYNORYJVMWNFXMVWRODBYSMBTZISISRZBSRBOXYTQXNZKTVOPKBXOHXYMMIFMMSWIBSLSVJ\tPEHNRX\t0.4069\t13732\t3041632938449863492\tfalse\n" +
                "2015-03-10T00:10:00.000Z\tVTJWCP\t384.000000000000\tPGKJRQGKHQHXYUVDUZQTICMPWFZEINPQOGHUGZGDCFLNGCEFBTDNSYQTIGUTKIESOSYYLIBUFGPWTQJQWTGERXRSYZCKPFWECEH\t2015-03-10T00:09:50.000Z\t0.062803771347\t896.000000000000\tYVJISIQFNSEUHOSVSIKJFJLNEKTSLZFPGDVCLMZTXOYEPKECCJZJOSDCIWCZECJGNWQNKCYVZJRRZYDBL\tPEHNRX\t0.9202\t-15664\t-5743731661904518905\ttrue\n";

        try (AsOfPartitionedJoinRecordSource source = new AsOfPartitionedJoinRecordSource(
                compiler.compileSource("y")
                , 0
                , new NoRowidSource().of(compiler.compileSource("x"))
                , 0
                ,
                new CharSequenceHashSet() {{
                    add("ccy");
                }}
                , 512
        )) {
            printer.printCursor(source.prepareCursor(factory));
            TestUtils.assertEquals(expected, sink);
        }
    }

    private static class NoRowidSource implements RecordSource<Record> {
        private RecordSource<? extends Record> delegate;

        @Override
        public RecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @SuppressWarnings("unchecked")
        @Override
        public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
            return (RecordCursor<Record>) delegate.prepareCursor(factory);
        }

        @Override
        public void reset() {
            delegate.reset();
        }

        @Override
        public boolean supportsRowIdAccess() {
            return false;
        }

        public NoRowidSource of(RecordSource<? extends Record> delegate) {
            this.delegate = delegate;
            return this;
        }
    }
}
