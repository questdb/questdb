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
import com.nfsdb.ql.impl.AsOfJoinRecordSource;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Rnd;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsOfJoinRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        JournalWriter jw = factory.writer(new JournalStructure("x")
                        .$ts()
                        .$sym("ccy")
                        .$double("rate")
                        .$double("amount")
                        .$str("trader")
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

    @Test
    public void testJoin() throws Exception {
        final String expected = "2015-03-10T00:01:00.000Z\tSWHYRX\t0.000014240303\tEEKWXDYEYXVJJRXWCRMC\t\tNaN\tNaN\t\n" +
                "2015-03-10T00:02:00.000Z\tVTJWCP\t0.000000055333\tUXEMHBLSINWKKYIRJUXJJBSUNECEWZJUBYCNKIDENXZDHHKSNRJXELDMFPPNVFQFUEERIZMBDQPNNTYJCUPRPCMERQMJTUDNTSITJYQIBFMGNJONLQCHDUUHK\t2015-03-10T00:01:40.000Z\t192.000000000000\t0.000076212298\tUHLIHYBTVZNCLNXFSUWPNXHQUTZODWKOCPFYXPVKN\n" +
                "2015-03-10T00:03:00.000Z\tSWHYRX\t0.000000417286\tLXNXOTVPINMFNOVRTECNHHMKUCVRHWGMGSFTDBPFVOGBNIYQFMBRJDJEBDNQOKFPNSZBLBPYPYWEHFGRPSEP\t2015-03-10T00:02:50.000Z\t969.875000000000\t37.786396026611\tMHOWKCDNZNLCNGZTOYTOXRSFPVRQLGYDONNLITWGLFCYQWPKLHTIIGQEYYPDVRGRQGKNPHKO\n" +
                "2015-03-10T00:04:00.000Z\tPEHNRX\t-1024.000000000000\tFERZKBFFKKCQKONGFZRFTVMFMLCDEMUKYKUBGMPPKUNDCWRPFOGWOQHDDUCWJQTEHRNVWUDKYBMRNXZPTHQKXFYVZHGTWPSYCNKXZBE\t2015-03-10T00:03:30.000Z\t0.000025531916\t591.750000000000\tMKRZUDJGNNDES\n" +
                "2015-03-10T00:05:00.000Z\tSWHYRX\t0.826539844275\tEIZFEQLDWJGLDUTMUGDVOVLRJMFJGIFWXJSJKNQTZQDZKORSOUPTJXUHWXBKZXU\t2015-03-10T00:04:10.000Z\t-44.283203125000\t0.000000015657\tMDBDBFDDFCSREDSWXYYYVSYYEQBORDTQHVCVUYGMBMKSCPWLZKDMPVRHWUVMBPSVEZDYHDHR\n" +
                "2015-03-10T00:06:00.000Z\tVTJWCP\t0.015854972880\tBHNPXHKMMUXHXDDRZPXSTEMKDEMPEZVYCFIBRUTIIPO\t2015-03-10T00:05:10.000Z\t0.000340097991\t0.000790880178\tRQUJJFGQIZKMDCXYTRVYQNFPGSQIXMIIFSWXSDDJSPKSHRIHCTIVYIVCHUCVBNEFJBPTUZKIPREERHVWLZRRCQHCPVSWMCRUUHPJFBKJKSHJYSP\n" +
                "2015-03-10T00:07:00.000Z\tSWHYRX\t38.471153259277\tGSCPZIYPBLDDGLLFTVIPPKZBXRYWYOBOMSCVIVXMMRSSDFPTXLVPMUZLEXKEKPPOSYOHGGJCXSGGIJQLTVNKNLXWFKLLQZYLYBHOPDTIDV\t2015-03-10T00:05:40.000Z\t0.000155258400\t621.000000000000\tOOFKUNSHNROL\n" +
                "2015-03-10T00:08:00.000Z\tVTJWCP\t833.036804199219\tICFNDVIYNDBZTTWCJHUJYSNSDDQHZDQBYINLSUIMVQDCRPPVOHIZXWFRWQSXWQPZEJGNWSQLNIWMIPRJCBDWRZOVIFNHQGWLUJELMKQKTGLROFQKGOERPDYOKG\t2015-03-10T00:07:50.000Z\t0.000002020489\t0.000000006163\tXOCYFWMEZBPNNMZYULBZKXPTEFQGNXLFIUPZTUPLKPFMDSXMCKLSYTGCRSTLNYYEKNPNVV\n" +
                "2015-03-10T00:09:00.000Z\tPEHNRX\t485.666015625000\tRONVKHEFPWEULMUIMOUZUQFJTXKWPSSJIKXNEXVMOGGEKQGBTGIUHRHYVTWTTBRBTHIMDCRICKZPBBEDCLEWHMBMRLOQRGWBSSHYMUJXJBLGLIQBNWGOKWHKZGWJNR\t2015-03-10T00:06:50.000Z\t96.448684692383\t554.543273925781\t\n" +
                "2015-03-10T00:10:00.000Z\tPEHNRX\t942.000000000000\tNYNCVHMCTZJNXHEGFCYZCTETZQRXSTLOBBHUKLCUDFSTDVSHUOQFPUNBUQCEXBOKFQEYZTVWVDWWXZJHTELGCJDVSSTJLTPFUNUIKKHKHJQTVCUVQZMERFEJQM\t2015-03-10T00:09:40.000Z\t0.000006120900\t0.000001666620\tNKFLRKBKRIBZVOJMFRIRPCTCXHCFNJFSFZLYPSNVQMRQJLVKFSPMPIOWMJVMIPO\n";

        AsOfJoinRecordSource source = new AsOfJoinRecordSource(
                compiler.compileSource("y")
                , 0
                , compiler.compileSource("x")
                , 0
                ,
                new ObjList<CharSequence>() {{
                    add("ccy");
                }}
        );

        printer.printCursor(source.prepareCursor(factory));

        TestUtils.assertEquals(expected, sink);
    }
}
