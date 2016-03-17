/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.sort;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Rnd;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class RBTreeSortedRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        Rnd rnd = new Rnd();
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("xyz")
                .$int("i")
                .$str("str")
                .$())) {
            int n = 100;

            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, rnd.nextInt());
                ew.putStr(1, rnd.nextChars(15));
                ew.append();
            }
            w.commit();
        }

        try (JournalWriter w = factory.bulkWriter(new JournalStructure("dupes")
                .$int("x")
                .$()
        )) {
            for (int i = 0; i < 10; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, i % 2 == 0 ? 10 : 20);
                ew.append();
            }
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, 30);
            ew.append();
            w.commit();
        }
    }

    @Test
    public void testEqualRows() throws Exception {
        assertThat("10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "30\n",
                "dupes order by x");

    }

    @Test
    public void testNestedOrderBy() throws Exception {
        final String expected = "-623522984\tCDNZNLCNGZTOYTO\n" +
                "-240272352\tCQSCMONRCXNUZFN\n" +
                "-485950131\tDILELRUMMZSCJOU\n" +
                "-111411421\tDSWLUVDRHFBCZIO\n" +
                "-1538602195\tDZJMYICCXZOUICW\n" +
                "-1885508206\tEISQHNOJIGFINKG\n" +
                "-1798281915\tFNPOYNNCTFSNSXH\n" +
                "-1980222246\tFOQKYHQQUWQOEEN\n" +
                "-1101822104\tFYUDEYYQEHBHFOW\n" +
                "-1871096754\tGPPRGSXBHYSBQYM\n" +
                "-772867311\tIGQZVKHTLQZSLQV\n" +
                "-618037497\tIOVIKJSMSSUQSRL\n" +
                "-2143484802\tJTHMHZNVZHCNXZE\n" +
                "-873768107\tJUHCLQCMZCCYVBD\n" +
                "-1129894630\tKFDONPWUVJWXEQX\n" +
                "-1201923128\tKGHVUVSDOTSEDYY\n" +
                "-1559759819\tKVZIEBSQCNSFFLT\n" +
                "-230310857\tLFWZSGDIRDLRKIW\n" +
                "-712888431\tLNEJRMDIKDISGQF\n" +
                "-1078921487\tLRHWQXYPOVFDBZW\n" +
                "-2105201404\tMBEZGHWVDKFLOPJ\n" +
                "-1234141625\tNDCQCEHNOMVELLK\n" +
                "-996446838\tNFCLTJCKFMQNTOG\n" +
                "-1023667478\tOYPHRIPZIMNZZRM\n" +
                "-422941535\tPDXYSBEOUOJSHRU\n" +
                "-1599598676\tPMOOUHWUDVIKRPC\n" +
                "-1440731932\tPRMDBDBFDDFCSRE\n" +
                "-1380922973\tQPZGPZNYVLTPKBB\n" +
                "-1895669864\tQYTEWHUWZOOVPPL\n" +
                "-1182156192\tSMPGLUOHNZHZSQL\n" +
                "-667031149\tSRYRFBVTMHGOOZZ\n" +
                "-1768335227\tSWUGSHOLNVTIQBZ\n" +
                "-46850431\tTDCEBYWXBBZVRLP\n" +
                "-1148479920\tTJWCPSWHYRXPEHN\n" +
                "-1119345664\tTTKIBWFCKDHBQJP\n" +
                "-780111957\tTWGLFCYQWPKLHTI\n" +
                "-76702538\tUIGENFELWWRSLBM\n" +
                "-670048539\tVFUUTOMFUIOXLQL\n" +
                "-447593202\tVRVNGSTEQODRZEI\n" +
                "-1650060090\tVZWEVQTQOZKXTPN\n" +
                "-1204896732\tWHPZRHHMGZJYYFL\n" +
                "-2108151088\tXPKRGIIHYHBOQMY\n" +
                "-485549586\tXUKLGMXSLUQDYOP\n" +
                "-1534034235\tYLPGZHITQJLKTRD\n" +
                "-388575268\tYXYGYFUXCDKDWOM\n" +
                "-6941891\tYZUZYJIHZBWWXFQ\n" +
                "-1424393416\tZUDJGNNDESHYUME\n";

//        assertThat("", "xyz where str = 'YZUZYJIHZBWWXFQ'");
        assertThat(expected, "(xyz where i < 100 order by i) order by str, i");
    }
}