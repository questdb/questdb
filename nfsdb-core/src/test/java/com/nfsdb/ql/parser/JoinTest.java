/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p>
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.IntHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.ql.model.QueryModel;
import com.nfsdb.ql.model.Statement;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class JoinTest {
    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));

    private final QueryParser parser = new QueryParser();
    private final RecordSourceBuilder joinOptimiser = new RecordSourceBuilder();
    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter printer = new RecordSourcePrinter(sink);

    @BeforeClass
    public static void setUp() throws Exception {
        generateJoinData();
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        try {
            parser.setContent("orders join customers on customerId = customerId");
            Statement statement = parser.parse();
            joinOptimiser.optimise(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(25, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Ambiguous"));
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        final String expected = "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1605271283\t9619\t486\t\t2015-07-10T00:00:29.443Z\tYM\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t401073894\t9619\t1645\tDND\t2015-07-10T00:00:31.115Z\tFNMURHFGESODNWN\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t921021073\t9619\t1860\tSR\t2015-07-10T00:00:41.263Z\tOJXJCNBLYTOIYI\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1986641415\t9619\t935\tUFUC\t2015-07-10T00:00:50.470Z\tFREQGOPJK\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1635896684\t9619\t228\tRQLVWE\t2015-07-10T00:00:51.036Z\tMZCMZMHGTIQ\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t189633559\t9619\t830\tRQMR\t2015-07-10T00:01:20.166Z\tQPL\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t960875992\t9619\t960\t\t2015-07-10T00:01:29.735Z\tYJZPHQDJKOM\n";

        assertQuery(expected, "customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'");
    }

    @Test
    public void testInvalidAlias() throws Exception {
        try {
            parser.setContent("orders join customers on orders.customerId = c.customerId");
            Statement statement = parser.parse();
            joinOptimiser.optimise(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(45, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("alias"));
        }
    }

    @Test
    public void testInvalidColumn() throws Exception {
        try {
            parser.setContent("orders join customers on customerIdx = customerId");
            Statement statement = parser.parse();
            joinOptimiser.optimise(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(25, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Invalid column"));
        }
    }

    @Test
    public void testInvalidTableName() throws Exception {
        try {
            parser.setContent("orders join customer on customerId = customerId");
            Statement statement = parser.parse();
            joinOptimiser.optimise(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(12, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Journal does not exist"));
        }
    }

    @Test
    public void testJoinSubQuery() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 1[ inner ] subquery ON customerName = orderId\n" +
                        "\n",
                "orders" +
                        " cross join (select customerId, customerName from customers where customerName ~ 'X')" +
                        " where orderId = customerName");

    }

    @Test
    public void testJoinCycle() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 3[ inner ] products (filter: products.productId = products.supplier) ON products.supplier = orders.orderId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "+ 2[ inner ] d (filter: d.orderId = d.productId) ON d.productId = orders.orderId\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.customerId\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where orders.orderId = suppliers.supplier");
    }

    @Test
    public void testJoinImpliedCrosses() throws Exception {
        assertPlan("+ 3[ cross ] products\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "+ 0[ cross ] orders\n" +
                        "+ 1[ cross ] customers\n" +
                        "+ 2[ cross ] d\n" +
                        "\n",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on 2=2" +
                        " join products on 3=3" +
                        " join suppliers on products.supplier = suppliers.supplier");
    }

    @Test
    public void testJoinMultipleFields() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.customerId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId and d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinOneFieldToTwo() throws Exception {
        assertPlan("+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.orderId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = customers.customerId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinOneFieldToTwoAcross() throws Exception {
        assertPlan("+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.orderId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = customers.customerId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on orders.orderId = d.orderId and d.orderId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinOneFieldToTwoAcross2() throws Exception {
        assertPlan("+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.orderId\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = customers.customerId and orders.orderId = d.orderId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinOneFieldToTwoReorder() throws Exception {
        assertPlan("+ 0[ cross ] orders (filter: orders.orderId = orders.customerId)\n" +
                        "+ 2[ inner ] customers ON customers.customerId = orders.customerId\n" +
                        "+ 1[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.customerId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinReorder() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.orderId\n" +
                        "+ 1[ inner ] customers ON customers.customerId = d.productId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinReorder3() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 2[ inner ] shippers ON shippers.shipper = orders.orderId\n" +
                        "+ 3[ inner ] d (filter: d.productId = d.orderId) ON d.productId = shippers.shipper and d.orderId = orders.orderId\n" +
                        "+ 5[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "+ 1[ cross ] customers\n" +
                        "\n",
                "orders" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinReorderRoot() throws Exception {
        assertPlan(
                "+ 0[ cross ] customers\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId\n" +
                        "+ 1[ inner ] orders ON orders.orderId = d.orderId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinReorderRoot2() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 2[ inner ] shippers ON shippers.shipper = orders.orderId\n" +
                        "+ 3[ inner ] d (filter: d.productId = d.orderId) ON d.productId = shippers.shipper and d.orderId = orders.orderId\n" +
                        "+ 4[ inner ] products ON products.productId = d.productId\n" +
                        "+ 5[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "+ 1[ cross ] customers\n" +
                        "\n",
                "orders" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId");
    }

    @Test
    public void testJoinWithFilter() throws Exception {
        assertPlan("+ 0[ cross ] customers\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId (post-filter: d.quantity < orders.orderId)\n" +
                        "+ 1[ inner ] orders ON orders.orderId = d.orderId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId (post-filter: products.price > d.quantity or d.orderId = orders.orderId)\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
                        "\n",
                "customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId" +
                        " and (products.price > d.quantity or d.orderId = orders.orderId) and d.quantity < orders.orderId");
    }

    @Test
    public void testOuterData() throws Exception {
        final String expected = "162\tGMRIFLMITGDYEV\t\tnull\tSPEKZKSGOBNGGYCMQDTJB\tBL\tVFZF\t2015-07-10T00:00:00.162Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "209\tFBWP\tYENLEZTSMCKFERVG\tnull\tDZDUV\tCYYVYQ\tJSMK\t2015-07-10T00:00:00.209Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "233\tQWZMQEFI\tTDFXHNFPWKMILOGWL\tnull\tDHCYX\tCUYVUVQRY\tGIJYDVRV\t2015-07-10T00:00:00.233Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "381\tVYPDCYB\tBYTCJGCTLGGHVJXGRILSCO\tnull\tIGMKIOPKMYIHL\tVEMYSYSTQEL\tQHNOJIGFINKGQVZ\t2015-07-10T00:00:00.381Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "396\tKDNTVGUZLQQPM\tDHUJJIJMEXUJSTGUGVUKEWQPQUYC\tnull\tUZVVKSKZJOPLFKJDKGYWDVYCBRNKJOHMUUKV\tRTVTODMNWELRV\tBUYZVQ\t2015-07-10T00:00:00.396Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "410\tUWWBTOWPJTW\tPYZZYPFVTUDCCBOGCSHVQDTJSKFDF\tnull\tEIUHQZLPDWWIMMRZPZPTWDWXVFRNXZERBBG\tMNJVHP\tJSMK\t2015-07-10T00:00:00.410Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "805\tJDD\tPBJDTSRWFDYKFUKNSMFUREMGUGV\tnull\tGU\tYZ\tOEENNEBQQEMXD\t2015-07-10T00:00:00.805Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "810\tJNDVPITEWCB\tWWTURRWZVQXZZYDGZNZCTQYIXQRLRW\tnull\tWMXWFJFIKXWTHBJGUTVIXQIVZYEBZYP\tEOGVU\tL\t2015-07-10T00:00:00.810Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1162\tZJSXCVDFRBBYP\tEDBFKPKBHQB\tnull\tZPNNILHNWDCKSHXQXKXOKTYNYTUISPYKRXNBGPIHZNDPRHNPHYIHGHKKZQID\tSJE\tTFSNSXH\t2015-07-10T00:00:01.162Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1344\tZGY\tQN\tnull\tNROGZBSXJTNYED\tGGCOBQZUM\tBOQMYSSMP\t2015-07-10T00:00:01.344Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1406\tVQHYIIQL\tKJE\tnull\tDYQFLMPNGEJKKJCRCKNPUTHTVNYXM\tDFDISBFBRCCQDV\tXTGNJ\t2015-07-10T00:00:01.406Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1414\tDHMTF\tRTGV\tnull\tHMDJSBGPXQTKPGGWFSTJSKSZSBEPDVNMFEVEMQCOHDBK\tJKBVDSERXZ\tOEENNEBQQEMXD\t2015-07-10T00:00:01.414Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1422\tEKVFCZGGCKCHK\tEEGCRISJP\tnull\tILWBREVXDHHWPREIFCMLRXSWMFWEKIOTXUPRNGEPIJVNKTXHNKYITYG\tYYBJER\tCNSFFLTRY\t2015-07-10T00:00:01.422Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1523\tV\tODSBTX\tnull\tECUZSRJCTRJLH\tVBNHX\tZHEI\t2015-07-10T00:00:01.523Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1567\tBWPEIFNITZK\tBIJXCYTOHOQT\tnull\tYZMMSEZIPCOCZZUFYIVELTS\tFVPDDGSEK\tUN\t2015-07-10T00:00:01.567Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1589\tQJZGPSXOHTWXD\tEKVIBQFPUIPNFOGDPVUUUQ\tnull\tX\tBGDBEDEFD\tBCZIOLYLPG\t2015-07-10T00:00:01.589Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1689\tWQ\tWZOHOOLBREWPFXYMVTDRBND\tnull\tGORYPTSTPHWOCVTZWOMURLEHZWCMOFQFYWNXDGNZKSFQUSMOVSWPCONS\tSWBCPLXVLMQ\tTOGM\t2015-07-10T00:00:01.689Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1734\tPSBRCYXIJLGOC\tDNBEWBWJPMCRQBTOOJPMFWVCPYZVRK\tnull\tZNQJZSPFSMRNBSCSVJMGLUWTWYFNIKBBKL\tHEGJPSFHUWOTUIL\tOWLPDXYSBEOUOJ\t2015-07-10T00:00:01.734Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "1913\tBYYTTZDNBCBLR\tPXLCJNSUGIWGJWLYEQ\tnull\tCHUQVSFDBBLSEMNFVNGZMTYTMK\tIZTBCTJ\tGLHMLLEOY\t2015-07-10T00:00:01.913Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "2172\t\tSRXYONINGWNLG\tnull\tXYNFIBOCMCXCKDWDROMMCGKRWNNPZNKBELQJVSVGWUMYCKJFCBDBCVZPO\tWLZXCZYXJXQWZL\tMQH\t2015-07-10T00:00:02.172Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "2312\tZIBIM\tRINZFPZU\tnull\tKNPETVEWYFUGELZUNMEZVGRQJUYZSBETJSEDWFVROCMWRMLLHMVKXNGRIMPF\tZWWINRXPMIGS\tNIMYFF\t2015-07-10T00:00:02.312Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "2378\tLSKKQ\tSSULKFTWUSZQLMBPDRIOYBHT\tnull\tHWCEGCCJCFVKJFTPTTNVCZVMVQUOITXQWKSSOLGOZSQXWZT\tHSOQYCWEROZ\tPKLHTIIG\t2015-07-10T00:00:02.378Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "2385\tVJDLICNNECMY\tHMDUCFWCSXVFG\tnull\tEHBOTVVKOGHHKICBOPWMBHOKLPDCNX\tWUW\tZMZV\t2015-07-10T00:00:02.385Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "2990\tDJUPSVOGIUEDG\tGTMXBMRHDSULOUIHTKXHLONPZ\tnull\tVRYBNVGGYPCFCZFCOCVOULWXREBBQMWJMDT\tVESFSEMNXOUSI\tZEQGMPLUCFTL\t2015-07-10T00:00:02.990Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3036\tNBOKZPH\tSOXIOEC\tnull\tPHVNUNBYMROWMLWTKZIOQIZHHWVCBYWRVY\tWOVJTL\tUZY\t2015-07-10T00:00:03.036Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3171\tFJBUXYJCC\tFSCJELZPZCWRHKFRVIR\tnull\tXEWJYQDVHIPWPHDZKPEDGFYPNNNVPWLYLN\tJEOUSRULHULNDLC\tOZZVDZJMYIC\t2015-07-10T00:00:03.171Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3213\tLCOTYSUBPNV\tEHIDIOWKZJQNRVBLWXFCHQVO\tnull\tTITHBMKLEOOKZFSYVZUCDXHDTJJYRHNVWIPVELVFQPXQDSHZIQQJFXMOFJRTOZ\tCMJSTVNPFNWPPBW\tUZY\t2015-07-10T00:00:03.213Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3222\tLHXREYVN\tLKXQVJZUVWJIGUBKGBUGKRON\tnull\t\tHDRD\tOEZBRQ\t2015-07-10T00:00:03.222Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3226\tKXZMMCGO\tQPW\tnull\tWKOLUQVELGJURZBQTWMSBSSSZYVCWIYKVQEFUZDBWJWTXGI\t\tQHNOJIGFINKGQVZ\t2015-07-10T00:00:03.226Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3270\tNDRWGFXTNNTQB\tCGYIEZEQJEJPLPBHGFBVEBQYWCMG\tnull\tDOMUIZPVKDENO\tHXTNWWFVY\tTI\t2015-07-10T00:00:03.270Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3383\tWMTZDJKNXCYCG\tFSUHTMXYEG\tnull\tEPFM\tXNMJINXIJM\tTOGM\t2015-07-10T00:00:03.383Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3476\tDPCRPYJGXJPBUOD\tKITKUVSLNTJTPEFOTSPPQU\tnull\tWKTXNKOKJOJTLFNXTZTZZJPS\tRIWLMOI\tB\t2015-07-10T00:00:03.476Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3478\tWTYRQ\tG\tnull\tVTDFWKVOOUSTQBWBNTZWMNCIWGZDWHKQYEODUUEMWWQBD\tXJ\tEYYPDVRGRQG\t2015-07-10T00:00:03.478Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3502\tVWXXO\tSXJIBJXMHDTEJLKIIRELKZC\tnull\tSRSHKSEONQCYOFIMHBKGZKHMFE\tIQBSTDTCDNQE\tFNWGRMD\t2015-07-10T00:00:03.502Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3589\tJOQQE\tKRUHSSEWMBELUIM\tnull\tEBMHCMDMDFSKYPDVYH\tYDMFRUNZIUXLB\tUHNBCCPM\t2015-07-10T00:00:03.589Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3663\tUDX\tHXMRSDUZYTPYUYBC\tnull\tQDUTVPDLFNFHQZZGCELWJZIPRBSORVECERSVKCNGFWSDZHPDK\tWKOUXMRBLX\tHQQUW\t2015-07-10T00:00:03.663Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3703\tUDRFDIXFY\tSGU\tnull\tRYFLDHPETQWIKEWYKDWQEX\tFQTJW\tEBNDCQCEHNO\t2015-07-10T00:00:03.703Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3751\tEL\tTZXHXHPTEWHNUQWBYV\tnull\tHQGMTGIIZBMQPNMJMHFGLUXWJVHPXVXDYNSSDCBKOSUTN\tLHPWO\t\t2015-07-10T00:00:03.751Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3891\tWMJFMFIY\tRELGTJVOIDJ\tnull\tXDQMLYFXJI\tNERDCGSGIWS\tWUVMBPSVEZDYHDH\t2015-07-10T00:00:03.891Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "3935\tMPPLRO\tPLFVGXOJQWZUVOOBRMGKCKMFTTINB\tnull\tTIMNMLWUZSMHNHYUYYRPKR\tDXWLZFL\tTI\t2015-07-10T00:00:03.935Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4041\tHSNIJKWXG\tUJYWZE\tnull\tDVWYVDMCGKDCVRBVVZELVKOKJKYRXNQTFURDIBSZDNGWUJVNVUMVLJBI\tDOIKCLGLLTB\tTFSNSXH\t2015-07-10T00:00:04.041Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4059\tULPIS\t\tnull\tWXPNLDBUVDW\tSBDCUXKKC\tBUYZVQ\t2015-07-10T00:00:04.059Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4132\tBWHGDEXNZMBFKTH\tZNVYO\tnull\tMQKEHUWPEEKEHHNBUPRMSQIHBBJNOHRRBSXGYWU\tYBVJTLPSLKVXZNM\t\t2015-07-10T00:00:04.132Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4412\tKSLZBHBGLU\tYJOWHLNKRGBMTYILPWINI\tnull\tSISKLWIGOZRWPSGZD\tPZEGSDJ\tPKLHTIIG\t2015-07-10T00:00:04.412Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4432\tK\tPXOFTVRRDMQYG\tnull\tSQCZOONVRVXHDCUSRWWJVGEDCKYJMG\tCWRO\tLU\t2015-07-10T00:00:04.432Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4745\tDYPIOX\t\tnull\tYUWUIQCFHKERXRUYHKIJRWGLEHMBURBIBHCLBHOPC\tFFREMYVLF\tIHYBTVZNCLN\t2015-07-10T00:00:04.745Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4839\tFNJOWUJCQHYQ\tZTG\tnull\tTEIKMXBOWWEQBLZZZLEGWTFEVHZBHNEXBUWPSCS\tBCHPCUEDKSQ\tVRB\t2015-07-10T00:00:04.839Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4934\tMHZTPC\tNFCFMZDOTVPXCFNOZJRWEQY\tnull\tYQEGLX\tVSVLHDNIKPWNOX\tMZCCYVBDMQEH\t2015-07-10T00:00:04.934Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4963\tET\t\tnull\tLZTCYZKKVFOYHPGETSYSPFMKNULYMRISCWICQIOBWSPSMINXBYKXVZHKMRMYZ\tKCYKZDQ\tERSMKRZUDJGNNDE\t2015-07-10T00:00:04.963Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "4981\tOEGWN\tELCMYM\tnull\tUIOWZGCEXXZNBYPOXEUJCMIHBHKIRBDVOXUZMTVOQYUTUJL\tVSISP\tFNWGRMD\t2015-07-10T00:00:04.981Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5207\tZNKCJHXBWEYN\tJEJEHFREXOVGPH\tnull\tEVCHKS\tXVQYLMJELWJLHS\t\t2015-07-10T00:00:05.207Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5288\tIXH\tSLTVJIGLBMTCUXPMSWOEDT\tnull\tWYRNJECJSCPQDSHDJKTOTIUIPTNBYXTRZUMKGEZNPFLFTELYBSGI\tNOCZPXX\tHRI\t2015-07-10T00:00:05.288Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5387\tSSPKDHERWUNX\tSGJMTMDPMSUPETCMIQE\tnull\tFBICHHGNEVNKVGJVI\tHFVYNKH\tINLKFNUH\t2015-07-10T00:00:05.387Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5445\tR\tOJKCNJSMEQYCSKCKQRQWGEDD\tnull\tDQKRXXFOC\tGGSOBQQHR\tEJRMDIKDI\t2015-07-10T00:00:05.445Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5653\tCE\tWPDTMIXZNQIIUVIXUNYECF\tnull\tLGOQPTMORXJFRXYOPEQXGERTXUTEIPJQJFCWKIITNBPOVKFQI\tXUOCLBYIB\tBBUKOJ\t2015-07-10T00:00:05.653Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5684\t\tQJIDXPOCPTYYURUKSMCRMJMQQJB\tnull\tW\tISOXFJGEOHFCR\t\t2015-07-10T00:00:05.684Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "5956\tFPXYZW\tHWYQKHYTDVPVLLDYOPITHLLJG\tnull\tKFWQUXKKOTUIUHXELOOKDJIJROU\tSRCTDZS\tZYNPPBX\t2015-07-10T00:00:05.956Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6006\tV\tMSKSWOFNYUKWNBKRFWOONIONILJJEMG\tnull\tCWHSYMVZMCDHWSSQTTEXRCVSKXCBYEYIHTXVMCJCPBOFIFKKXNGIEO\tTDVGHGKIVGU\tZHCN\t2015-07-10T00:00:06.006Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6039\tFXNPVVURFMOLR\tOHBKYKVDMRSOSRCSEWEJKCFJJLXDQM\tnull\tNMZSWWPQUFIRDJOGGDZCIPYWLVPLYTWECLKE\tKCMOCZMMCDIL\tHSQSPZ\t2015-07-10T00:00:06.039Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6122\tSRHNNUBREDEWYQL\tUNVYXEFBXLJUIBCIZUB\tnull\tHCPVXCIBOCQPPWQUPOBEEHTNDJQWFMPRTUQIXBORZXTYN\tZKHUJELJITS\tUN\t2015-07-10T00:00:06.122Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6468\tSZTUTU\tQPXBOTFQLQXYDLW\tnull\tSLQTFNGHYIVHDSUWWNVTBBVYPHKIHVIEHTXPHIWSCWGLNEVPCIITFFCQI\tMSMIMLWZJCL\tJWCPSWHYRXPEHN\t2015-07-10T00:00:06.468Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6486\tUXUCRDEQ\tCSOUCDKZHSFLLFMSJKCHTZKP\tnull\tLJKCIGNLIWJRFLMFDQBNIBZUQECN\tWTRPRCHCD\tHQQUW\t2015-07-10T00:00:06.486Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6678\tTYJDHKUNEO\tCFZRMGKKQOMGMEGKMNVFVZSFQNIQUDO\tnull\tMIZDRNOJDELNJPNHVLTPFRTLVZUFNTSOGZZCNYBHSVWGNNIP\tNJDIVGC\tMRT\t2015-07-10T00:00:06.678Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6691\tWGXLXNKO\tLBYMWEZCHRSNYPTCCXKRLQB\tnull\tDHBHVQPYOEDJSOKKDZTUNHRMZYQZWBHMBUXPN\tYSYUCK\tOUICWEKGHV\t2015-07-10T00:00:06.691Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6728\tHSFLSXTYEWWWMD\tSXVIHKWQQBSUYEKLTRVMQFZO\tnull\tIGOQUCGCKOJSSRVJWBCPYYNEWTQKCTCERGHDDGWKLOXBFDEKPKRMDKKENJFN\tRZ\tZRPFMDVVG\t2015-07-10T00:00:06.728Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6826\tVJDZ\tMGXBJXCFJHWCFDOTZLCPJSLFXTYIM\tnull\tTVPRLEMKQZZIIPCFGUKKKOBNTRPYVHMRUDUDSGZXTIFXWRFULWVWOHHJDZ\tCR\tVMC\t2015-07-10T00:00:06.826Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "6926\tZYESDDCGOGFUO\tWRDFOEFONJSPFGTUCKMIZWXIE\tnull\tVFJBNWHQVHMPJKSTXZXPQTOIMSJNCNRLUGQW\tIYFKD\tTI\t2015-07-10T00:00:06.926Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7021\tNLKYHGO\tRHGHYBZOMJJBVLGGPHG\tnull\tFJQMODHRBKVLZWSJUWMLJFVUKIZJVZBBFNMXZOQJFWZDVYQCQXRUSBSDOIBLSS\tPQVCYTWYLEK\tOEZBRQ\t2015-07-10T00:00:07.021Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7053\tWNSMHNXS\tOBSJHVQXMOJ\tnull\tEFVJTGURNJJZJIMUREPGMEJXDXDERDRXIMFZHYCZVKTDTBPYJQQPGYLIKPPBL\tPOWZIJQNISFYFN\tLPTYXYGY\t2015-07-10T00:00:07.053Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7056\tGTLEUE\tQWBCXBXNDFRPOQLROOEXRSFPUSFN\tnull\tROFOTIRXJTZWT\tBVTVUBQ\tWUVMBPSVEZDYHDH\t2015-07-10T00:00:07.056Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7122\tVCCO\tOJDYZWTUYNGWQNWEGOXYMJ\tnull\tPTWRSXZVZRNYUQDXLLOQVRMNLZUQRYIIYYJZBZGBKBRELDNPCM\tWCECUHSTCHICVIW\tIHZBWWXF\t2015-07-10T00:00:07.122Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7175\t\tSMSPDCSPNTOUVUVWG\tnull\tMWVXGQOPEFIW\tRTCGYGSMWYFHIK\tUXI\t2015-07-10T00:00:07.175Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7262\tVIXUVVWCWWTZVTT\tRHWJQEDZTDLZZODEC\tnull\tJMYKVBGFRCCVBPIYJFGUO\tZINNITZWLZJ\tLPTYXYGY\t2015-07-10T00:00:07.262Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7347\tVJDIMVDXSZJGONW\tQEHBEOGUKYJKMDKXYVTB\tnull\tMKOZKMNYESGUFQEWJJONMJXZGYKDZOFNIYCVCIIBPDKRMFXJURGLV\tXKRKNOBFOHRVHHK\tHSQSPZ\t2015-07-10T00:00:07.347Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7473\tEIZFIQOXKQTL\tRMTGUNFYICLTNRPOINOKISQR\tnull\tUTKENDEUCPB\tDDQ\tQXSPEPTTKIB\t2015-07-10T00:00:07.473Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7478\tVKQEOSR\tCNBBUPHUEXWJURYENCJMSSZLDOUET\tnull\tDLEJKKEWPSMEVYFWRXFIISVPMQWCPJR\tXGXTKBZLML\tZYNPPBX\t2015-07-10T00:00:07.478Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7497\tLFQGLDWFRXXG\tMOTZVCZZCSN\tnull\tPLZSQJTXPZBYGCNVCLJJYZWVCDLBEQOLFDGEQIHMGQKTXOEWYHEWVYXCOGWJMCU\tHUSKKOVGLEYFTS\tVELLKKHTWNWI\t2015-07-10T00:00:07.497Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7513\tV\tNPKCEVYISMIEZ\tnull\tSHKVNSEIDEJZYBDRJXJXUHFWPHQDJGYGUBFWHHBPZXPTUZNZBZQM\t\tGSTEQODRZEIWFOQ\t2015-07-10T00:00:07.513Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7644\tPGZGVZCOFMMUE\tDKDC\tnull\tQSTEVZPEHUZVOWSTSWXBTUWEGQQIPN\tSQQGPYG\tUOUIGENFELWWRSL\t2015-07-10T00:00:07.644Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7737\tIXTXWXNRTEV\tOLMYQTMBOETE\tnull\tCSINBVKDS\tSVZXEVCRGW\tXGZMDJTHMHZN\t2015-07-10T00:00:07.737Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7911\tB\tHBDFCVYUERTYMXRVDKJHWG\tnull\tUXZBGJRUXHSDUDRIOCUIICKOZSVXHDYXEKDDVFTPGRQWIZYIOBMZFMYHZH\tLNXNEFZQJYO\tJBF\t2015-07-10T00:00:07.911Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7961\tFLBPTOLRBCSL\tXUE\tnull\tXGHYVHINXWPB\tYTMTUHYYRKUHLUQ\tVFZF\t2015-07-10T00:00:07.961Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7968\tRXSC\tHGHZDQS\tnull\tEGRPGPPRXJUPJLGCRETXZNNHBPNWRFBBHEMODPJZLDYKWBY\tIK\tRGOONFCLTJCKFMQ\t2015-07-10T00:00:07.968Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "7976\tDEXNQWBDJQEHUM\tQIFWWFNVZUZJNJQ\tnull\tKOIY\tWV\tFSUWPNXHQUT\t2015-07-10T00:00:07.976Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8074\tBFGSXPKVKURV\t\tnull\tTIGRLGDVMMCIMHXICXJWRWLPKCRQRQFIOTGHXHGVDIRMRUMV\tF\tPJEUKW\t2015-07-10T00:00:08.074Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8113\tDDGFSZHOG\tPBV\tnull\tXFBTRICSWEOBKHUFDRSNFDEXDFTMEHBKQPKRRQFPTZKFRXRGJWE\tFTID\t\t2015-07-10T00:00:08.113Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8121\tELZPVWLPQQO\tIJHLCIPMKJBHKRNYRRIBCWQTWHFMC\tnull\tJSCYBPOMOFRYEFHZNYNTTMLGFJHLLKIKMOMC\tLDGWBHIQNE\tINLKFNUH\t2015-07-10T00:00:08.121Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8130\tSYGBYFSNQOT\tINUCPVCRKPQSKZOSNQQSLKDELFSVVDI\tnull\tXQGGHWGJZDKHLDCJSBQYKXZUMWQLHUOTDZCEJRZPF\tYXKQOIT\tZI\t2015-07-10T00:00:08.130Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8199\tJWFODJS\tOMOIJIUGNUZMSXEK\tnull\tIFRCSLFUDRPRMLHTBXLFOVRZNTENWJMKFHOZEGVBX\tLFEQGRKKBPMI\tGSTEQODRZEIWFOQ\t2015-07-10T00:00:08.199Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8284\t\tJKYMSFW\tnull\tFZQPSJETVHJVVOIZRITTFEPZMEJHDXYGHWDNUEIIEXCFVTV\tMRMRGTUFNZHXKQY\tFEVHK\t2015-07-10T00:00:08.284Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8320\tWGCFDLNDIEW\tORDODPC\tnull\tTTHPTKYEYYP\tXOYOEMXIEGP\tOVFDBZWNI\t2015-07-10T00:00:08.320Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8379\tQQPSQOPG\tVLSFYVGGHMGVPIGUJCBZS\tnull\tZCRHPDJMWNSLOMQZXPNFMHGZKLUNEUUDMYBXWRFYRPIFIRSSEEINLIHHXSHZEZZ\tEW\tWUVMBPSVEZDYHDH\t2015-07-10T00:00:08.379Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8532\tCGOSJWM\tGZEFSMRLMUULNJDSOKJOR\tnull\t\tSHXMT\tHNZH\t2015-07-10T00:00:08.532Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8549\tXKGGQNZPZXDWF\tNFEBNUVEQJOPWKHYEXVWEMHNQKD\tnull\tLQRZKJEGFWQIEHNDQNSLLEJKYHHGCEF\t\tJFRPXZSF\t2015-07-10T00:00:08.549Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8567\tRUDDLZNHGVHZO\tKOZOOJWDDO\tnull\tRMJURFHFUWRFYYROGJUKEKJTIOKSOMWQCIGBXGTHJJBHFFFEUXJQPOGQL\tFFKJOOXKH\tRJUEBWV\t2015-07-10T00:00:08.567Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "8821\tZ\tHEDEUYVXYULDEDVOZTIXLRHX\tnull\tLBZKRTLMHHZQYJTHVKTHTQCZUTYNTSRIEFIVJIT\tBUOIOYPUMWUHEI\tTOGM\t2015-07-10T00:00:08.821Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9021\tMSM\tMJWVDCOWPQFCDB\tnull\tEFTSFYICZHJLOPKNJTSKMGPBIZUTTXSZCIBKIMNHPEEX\tMIKOFVNI\tVFZF\t2015-07-10T00:00:09.021Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9180\tDX\tLMHJXBDPMRECNYXZ\tnull\tTYDLLXJTKOFTUPQXUSEWRSQCLENIOMZPSW\tYS\t\t2015-07-10T00:00:09.180Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9752\t\tKIJZZXFXHTCOEJPWMICBQOXQQQPBGF\tnull\tSWSMBCZIORTGCWCFVXSYKTLDCDLJQEKZLJVTLIJSLXH\tCSRXHZCODL\tGSTEQODRZEIWFOQ\t2015-07-10T00:00:09.752Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9754\tVBUNKYVVGQLKQCD\tBOOFXUSIVEHEJIOTLOYTCCKIVVUHQ\tnull\tFGFXYRFCNLJEWVEQYHNESWJRBYQBQP\tIPEWMGFDPQH\tXLQLUUZIZKMFCKV\t2015-07-10T00:00:09.754Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9782\tOIGUBDZRIHVSSH\tFKXP\tnull\tYCDPIVPZVGZOTUBFPLOVJRWTQLQW\tRVYMWKMJEYMFNNL\tEVQTQ\t2015-07-10T00:00:09.782Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9977\tINDD\tMIRNGQQJMVOHKXHIYFMUMLXDGEQIEGT\tnull\tXULPFZVERXTYXHEIXXTUSJFKTHCHSXYTBLILZKTHJTFSFQPTUZHIURBOMPGHUR\tMUIYMRYE\tWLZKDMPVR\t2015-07-10T00:00:09.977Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                "9992\tHTEOO\tFEVRVVUBNQZINQYGSBOXQGTVNRLXE\tnull\tHJBNFHODEFEMCKSVYWTQPMEVLLSJINDCHSPTIZDB\tDXIUYZNJ\tORD\t2015-07-10T00:00:09.992Z\tNaN\tNaN\tNaN\t\t\tnull\n";

        assertQuery(expected, "customers c" +
                " outer join orders o on c.customerId = o.customerId " +
                " where orderId = NaN");
    }

    @Test
    public void testOuterExistence1() throws Exception {
        assertPlan("+ 0[ cross ] c\n" +
                        "+ 1[ outer ] o ON o.customerId = c.customerId (post-filter: null = orderId)\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where null = orderId");
    }

    @Test
    public void testOuterExistence2() throws Exception {
        assertPlan("+ 0[ cross ] c\n" +
                        "+ 1[ outer ] o ON o.customerId = c.customerId (post-filter: orderId = null)\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where orderId = null");
    }

    @Test
    public void testOuterExistence3() throws Exception {
        assertPlan("+ 0[ cross ] c\n" +
                        "+ 1[ outer ] o ON o.customerId = c.customerId (post-filter: orderId = NaN)\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where orderId = NaN");
    }

    private static void generateJoinData() throws JournalException {
        JournalWriter customers = factory.writer(
                new JournalStructure("customers").
                        $int("customerId").
                        $str("customerName").
                        $str("contactName").
                        $str("address").
                        $str("city").
                        $str("postalCode").
                        $sym("country").
                        $ts()
        );

        JournalWriter categories = factory.writer(
                new JournalStructure("categories").
                        $sym("category").index().valueCountHint(1000).
                        $str("description").
                        $ts()
        );

        JournalWriter employees = factory.writer(
                new JournalStructure("employees").
                        $str("employeeId").index().buckets(2048).
                        $str("firstName").
                        $str("lastName").
                        $date("birthday").
                        $ts()
        );

        JournalWriter orderDetails = factory.writer(
                new JournalStructure("orderDetails").
                        $int("orderDetailId").
                        $int("orderId").
                        $int("productId").
                        $int("quantity").
                        $ts()
        );

        JournalWriter orders = factory.writer(
                new JournalStructure("orders").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").index().
                        $ts("orderDate").
                        $sym("shipper").
                        $()
        );

        JournalWriter products = factory.writer(
                new JournalStructure("products").
                        $int("productId").
                        $str("productName").
                        $sym("supplier").index().valueCountHint(1000).
                        $sym("category").index().valueCountHint(1000).
                        $double("price").
                        $ts()
        );


        JournalWriter shippers = factory.writer(
                new JournalStructure("shippers").
                        $sym("shipper").
                        $str("phone").
                        $ts()
        );

        JournalWriter suppliers = factory.writer(
                new JournalStructure("suppliers").
                        $sym("supplier").valueCountHint(1000).
                        $str("contactName").
                        $str("address").
                        $str("city").
                        $str("postalCode").
                        $sym("country").index().
                        $str("phone").
                        $ts()
        );

        final Rnd rnd = new Rnd();
        long time = Dates.parseDateTime("2015-07-10T00:00:00.000Z");

        // statics
        int countryCount = 196;
        ObjList<String> countries = new ObjList<>();
        for (int i = 0; i < countryCount; i++) {
            countries.add(rnd.nextString(rnd.nextInt() & 15));
        }

        IntHashSet blackList = new IntHashSet();
        // customers
        int customerCount = 10000;
        for (int i = 0; i < customerCount; i++) {

            if (rnd.nextPositiveInt() % 100 == 0) {
                blackList.add(i);
            }

            JournalEntryWriter w = customers.entryWriter();
            w.putInt(0, i);
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(2, rnd.nextChars(rnd.nextInt() & 31));
            w.putStr(4, rnd.nextChars(rnd.nextInt() & 63));
            w.putStr(5, rnd.nextChars(rnd.nextInt() & 15));
            w.putSym(6, countries.getQuick(rnd.nextPositiveInt() % 196));
            w.putDate(7, time++);
            w.append();
        }
        customers.commit();

        // categories
        for (int i = 0; i < 100; i++) {
            JournalEntryWriter w = categories.entryWriter();
            w.putSym(0, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 63));
            w.putDate(2, time++);
            w.append();
        }
        categories.commit();

        // employees
        int employeeCount = 2000;
        for (int i = 0; i < employeeCount; i++) {
            JournalEntryWriter w = employees.entryWriter();
            w.putStr(0, rnd.nextChars(rnd.nextInt() & 7));
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(2, rnd.nextChars(rnd.nextInt() & 15));
            w.putDate(3, 0);
            w.putDate(4, time++);
            w.append();
        }
        employees.commit();

        // suppliers
        for (int i = 0; i < 100; i++) {
            JournalEntryWriter w = suppliers.entryWriter();
            w.putSym(0, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(2, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(3, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(4, rnd.nextChars(rnd.nextInt() & 7));
            w.putSym(5, countries.getQuick(rnd.nextPositiveInt() % countryCount));
            w.putStr(6, rnd.nextChars(rnd.nextInt() & 15));
            w.putDate(7, time++);
            w.append();
        }
        suppliers.commit();

        SymbolTable categoryTab = categories.getSymbolTable("category");
        int categoryTabSize = categoryTab.size();
        SymbolTable supplierTab = suppliers.getSymbolTable("supplier");
        int supplierTabSize = supplierTab.size();

        // products
        int productCount = 2000;
        for (int i = 0; i < productCount; i++) {
            JournalEntryWriter w = products.entryWriter();
            w.putInt(0, i);
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 15));
            w.putSym(2, supplierTab.value(rnd.nextPositiveInt() % supplierTabSize));
            w.putSym(3, categoryTab.value(rnd.nextPositiveInt() % categoryTabSize));
            w.putDouble(4, rnd.nextDouble());
            w.putDate(5, time++);
            w.append();
        }
        products.commit();

        // shippers
        for (int i = 0; i < 20; i++) {
            JournalEntryWriter w = shippers.entryWriter();
            w.putSym(0, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 7));
            w.append();
        }
        shippers.commit();

        SymbolTable shipperTab = shippers.getSymbolTable("shipper");
        int shipperTabSize = shipperTab.size();

        int d = 0;
        for (int i = 0; i < 100000; i++) {
            int customerId = rnd.nextPositiveInt() % customerCount;
            if (blackList.contains(customerId)) {
                continue;
            }

            int orderId = rnd.nextPositiveInt();
            JournalEntryWriter w = orders.entryWriter(time++);

            w.putInt(0, orderId);
            w.putInt(1, customerId);
            w.putInt(2, rnd.nextPositiveInt() % productCount);
            w.putStr(3, employees.getPartition(0, true).getFlyweightStr(rnd.nextPositiveLong() % employeeCount, 0));
            w.putSym(5, shipperTab.value(rnd.nextPositiveInt() % shipperTabSize));
            w.append();

            int k = (rnd.nextInt() & 3) + 1;

            for (int n = 0; n < k; n++) {
                JournalEntryWriter dw = orderDetails.entryWriter();
                dw.putInt(0, ++d);
                dw.putInt(1, orderId);
                dw.putInt(2, rnd.nextPositiveInt() % productCount);
                dw.putInt(3, (rnd.nextInt() & 3) + 1);
                dw.append();
            }
        }
        orders.commit();
        orderDetails.commit();
    }

    private void assertPlan(String expected, String query) throws ParserException, JournalException {
        parser.setContent(query);
        QueryModel model = parser.parse().getQueryModel();
        joinOptimiser.optimise(model, factory);
        TestUtils.assertEquals(expected, joinOptimiser.plan(model));
    }

    private void assertQuery(String expected, String query) throws ParserException, JournalException {
        sink.clear();
        parser.setContent(query);
        QueryModel model = parser.parse().getQueryModel();
        joinOptimiser.optimise(model, factory);
        printer.print(joinOptimiser.compileX(model, factory), factory);
        TestUtils.assertEquals(expected, sink);
    }
}
