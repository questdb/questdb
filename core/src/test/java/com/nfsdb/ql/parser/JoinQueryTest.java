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

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Rnd;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.impl.NoRowidSource;
import com.nfsdb.ql.impl.join.HashJoinRecordSource;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.SymbolTable;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JoinQueryTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        generateJoinData();
    }

    @Test
    public void testAllOrdersForACustomers() throws Exception {
        assertThat("32209860\t1\t1000\tMZPFIR\t2015-07-10T00:00:17.050Z\tOJXJCNBLYTOIYI\n" +
                        "1020826110\t1\t1884\tGRG\t2015-07-10T00:00:21.152Z\tQXOLEEXZ\n" +
                        "1921430865\t1\t1682\tXV\t2015-07-10T00:00:27.676Z\tMZCMZMHGTIQ\n" +
                        "355660772\t1\t1766\tOFN\t2015-07-10T00:00:33.004Z\tW\n" +
                        "1078912382\t1\t1296\tYBHPRQD\t2015-07-10T00:00:54.661Z\tYJZPHQDJKOM\n" +
                        "1173798559\t1\t387\t\t2015-07-10T00:01:00.700Z\tKZRCKS\n" +
                        "132190528\t1\t1088\t\t2015-07-10T00:01:07.110Z\tFBLGGTZEN\n" +
                        "385427654\t1\t1703\tXEMP\t2015-07-10T00:01:33.250Z\tMZCMZMHGTIQ\n",
                "orders where customerId = 1 ");
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        try {
            assertPlan("", "orders join customers on customerId = customerId");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(25, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Ambiguous"));
        }
    }

    @Test
    public void testAsOfJoinOrder() throws Exception {
        assertPlan("+ 0[ cross ] c\n" +
                        "+ 1[ asof ] e ON e.employeeId = c.customerId\n" +
                        "+ 2[ inner ] o ON o.customerId = c.customerId\n" +
                        "\n",
                "customers c" +
                        " asof join employees e on c.customerId = e.employeeId" +
                        " join orders o on c.customerId = o.customerId");

    }

    @Test
    public void testCount() throws Exception {
        assertThat("162\t1\n" +
                        "209\t1\n" +
                        "233\t1\n" +
                        "381\t1\n" +
                        "396\t1\n" +
                        "410\t1\n" +
                        "805\t1\n" +
                        "810\t1\n" +
                        "1162\t1\n" +
                        "1344\t1\n" +
                        "1406\t1\n" +
                        "1414\t1\n" +
                        "1422\t1\n" +
                        "1523\t1\n" +
                        "1567\t1\n" +
                        "1589\t1\n" +
                        "1689\t1\n" +
                        "1734\t1\n" +
                        "1913\t1\n" +
                        "2172\t1\n" +
                        "2312\t1\n" +
                        "2378\t1\n" +
                        "2385\t1\n" +
                        "2990\t1\n" +
                        "3036\t1\n" +
                        "3171\t1\n" +
                        "3213\t1\n" +
                        "3222\t1\n" +
                        "3226\t1\n" +
                        "3270\t1\n" +
                        "3383\t1\n" +
                        "3476\t1\n" +
                        "3478\t1\n" +
                        "3502\t1\n" +
                        "3589\t1\n" +
                        "3663\t1\n" +
                        "3703\t1\n" +
                        "3751\t1\n" +
                        "3891\t1\n" +
                        "3935\t1\n" +
                        "4041\t1\n" +
                        "4059\t1\n" +
                        "4132\t1\n" +
                        "4412\t1\n" +
                        "4432\t1\n" +
                        "4745\t1\n" +
                        "4839\t1\n" +
                        "4934\t1\n" +
                        "4963\t1\n" +
                        "4981\t1\n" +
                        "5207\t1\n" +
                        "5288\t1\n" +
                        "5387\t1\n" +
                        "5445\t1\n" +
                        "5653\t1\n" +
                        "5684\t1\n" +
                        "5956\t1\n" +
                        "6006\t1\n" +
                        "6039\t1\n" +
                        "6122\t1\n" +
                        "6468\t1\n" +
                        "6486\t1\n" +
                        "6678\t1\n" +
                        "6691\t1\n" +
                        "6728\t1\n" +
                        "6826\t1\n" +
                        "6926\t1\n" +
                        "7021\t1\n" +
                        "7053\t1\n" +
                        "7056\t1\n" +
                        "7122\t1\n" +
                        "7175\t1\n" +
                        "7262\t1\n" +
                        "7347\t1\n" +
                        "7473\t1\n" +
                        "7478\t1\n" +
                        "7497\t1\n" +
                        "7513\t1\n" +
                        "7644\t1\n" +
                        "7737\t1\n" +
                        "7911\t1\n" +
                        "7961\t1\n" +
                        "7968\t1\n" +
                        "7976\t1\n" +
                        "8074\t1\n" +
                        "8113\t1\n" +
                        "8121\t1\n" +
                        "8130\t1\n" +
                        "8199\t1\n" +
                        "8284\t1\n" +
                        "8320\t1\n" +
                        "8379\t1\n" +
                        "8532\t1\n" +
                        "8549\t1\n" +
                        "8567\t1\n" +
                        "8821\t1\n" +
                        "9021\t1\n" +
                        "9180\t1\n" +
                        "9752\t1\n" +
                        "9754\t1\n" +
                        "9782\t1\n" +
                        "9977\t1\n" +
                        "9992\t1\n",
                "select c.customerId, count() from customers c" +
                        " outer join orders o on c.customerId = o.customerId " +
                        " where o.customerId = NaN");
    }

    @Test
    public void testDuplicateAlias() throws Exception {
        try {
            assertPlan("", "customers a" +
                    " cross join orders a");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(30, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Duplicate"));
        }
    }

    @Test
    public void testDuplicateJournals() throws Exception {
        assertPlan("+ 0[ cross ] customers\n" +
                        "+ 1[ cross ] customers\n" +
                        "\n",
                "customers" +
                        " cross join customers");
    }

    @Test
    public void testEqualsConstantTransitivityLhs() throws Exception {
        assertPlan("+ 0[ cross ] c (filter: 100 = c.customerId)\n" +
                        "+ 1[ outer ] o (filter: o.customerId = 100) ON o.customerId = c.customerId\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where 100 = c.customerId");
    }

    @Test
    public void testEqualsConstantTransitivityRhs() throws Exception {
        assertPlan("+ 0[ cross ] c (filter: c.customerId = 100)\n" +
                        "+ 1[ outer ] o (filter: o.customerId = 100) ON o.customerId = c.customerId\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where c.customerId = 100");
    }

    @Test
    public void testExceptionOnIntLatestByWithoutFilter() throws Exception {
        try {
            assertThat("", "orders latest by customerId");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(17, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Only SYM columns"));
        }
    }

    @Test
    public void testGenericPreFilterPlacement() throws Exception {
        assertPlan("+ 0[ cross ] customers (filter: customerName ~ 'WTBHZVPVZZ')\n" +
                        "+ 1[ inner ] orders ON orders.customerId = customers.customerId\n" +
                        "\n",
                "select customerName, orderId, productId " +
                        "from customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'");
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

        assertThat(expected, "customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'");
    }

    @Test
    public void testInnerJoinEqualsConstant() throws Exception {
        final String expected = "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1605271283\t9619\t486\t\t2015-07-10T00:00:29.443Z\tYM\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t401073894\t9619\t1645\tDND\t2015-07-10T00:00:31.115Z\tFNMURHFGESODNWN\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t921021073\t9619\t1860\tSR\t2015-07-10T00:00:41.263Z\tOJXJCNBLYTOIYI\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1986641415\t9619\t935\tUFUC\t2015-07-10T00:00:50.470Z\tFREQGOPJK\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1635896684\t9619\t228\tRQLVWE\t2015-07-10T00:00:51.036Z\tMZCMZMHGTIQ\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t189633559\t9619\t830\tRQMR\t2015-07-10T00:01:20.166Z\tQPL\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t960875992\t9619\t960\t\t2015-07-10T00:01:29.735Z\tYJZPHQDJKOM\n";

        assertThat(expected, "customers join orders on customers.customerId = orders.customerId where customerName = 'WTBHZVPVZZ'");
    }

    @Test
    public void testInnerJoinEqualsConstantLhs() throws Exception {
        final String expected = "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1605271283\t9619\t486\t\t2015-07-10T00:00:29.443Z\tYM\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t401073894\t9619\t1645\tDND\t2015-07-10T00:00:31.115Z\tFNMURHFGESODNWN\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t921021073\t9619\t1860\tSR\t2015-07-10T00:00:41.263Z\tOJXJCNBLYTOIYI\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1986641415\t9619\t935\tUFUC\t2015-07-10T00:00:50.470Z\tFREQGOPJK\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t1635896684\t9619\t228\tRQLVWE\t2015-07-10T00:00:51.036Z\tMZCMZMHGTIQ\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t189633559\t9619\t830\tRQMR\t2015-07-10T00:01:20.166Z\tQPL\n" +
                "9619\tWTBHZVPVZZ\tT\tnull\tBMUPYPIZEPQKHZNGZGBUWDS\tPNKVDJOF\tFLRBROMNXKU\t2015-07-10T00:00:09.619Z\t960875992\t9619\t960\t\t2015-07-10T00:01:29.735Z\tYJZPHQDJKOM\n";

        assertThat(expected, "customers join orders on customers.customerId = orders.customerId where 'WTBHZVPVZZ' = customerName");
    }

    @Test
    public void testInnerJoinSubQuery() throws Exception {
        final String expected =
                "WTBHZVPVZZ\tBOKHCKYDMWZ\t1605271283\n" +
                        "WTBHZVPVZZ\tKD\t401073894\n" +
                        "WTBHZVPVZZ\tBWJG\t921021073\n" +
                        "WTBHZVPVZZ\tK\t1986641415\n" +
                        "WTBHZVPVZZ\tBXWG\t1635896684\n" +
                        "WTBHZVPVZZ\tKRBCWYMNOQS\t189633559\n" +
                        "WTBHZVPVZZ\tFEOLY\t960875992\n";

        assertThat(expected, "select customerName, productName, orderId from (" +
                "select customerName, orderId, productId " +
                "from customers join orders on customers.customerId = orders.customerId where customerName ~ 'WTBHZVPVZZ'" +
                ") x" +
                " join products p on p.productId = x.productId");

        assertThat(expected, "select customerName, productName, orderId " +
                " from customers join orders o on customers.customerId = o.customerId " +
                " join products p on p.productId = o.productId" +
                " where customerName ~ 'WTBHZVPVZZ'");
    }

    @Test
    public void testIntrinsicFalse() throws Exception {
        assertThat("customerName\tproductName\torderId\n",
                "select customerName, productName, orderId " +
                        " from customers join orders o on customers.customerId = o.customerId " +
                        " join products p on p.productId = o.productId" +
                        " where customerName ~ 'WTBHZVPVZZ' and 1 > 1", true);
    }

    @Test
    public void testInvalidAlias() throws Exception {
        try {
            assertPlan("", "orders join customers on orders.customerId = c.customerId");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "alias"));
        }
    }

    @Test
    public void testInvalidColumn() throws Exception {
        try {
            assertPlan("", "orders join customers on customerIdx = customerId");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(25, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Invalid column"));
        }
    }

    @Test
    public void testInvalidSelectColumn() throws Exception {
        try {
            compiler.compile(factory, "select c.customerId, orderIdx, o.productId from " +
                    "customers c " +
                    "join (" +
                    "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                    ") o on c.customerId = o.customerId");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(21, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Invalid column"));
        }

        try {
            compiler.compile(factory, "select c.customerId, orderId, o.productId2 from " +
                    "customers c " +
                    "join (" +
                    "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                    ") o on c.customerId = o.customerId");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(30, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Invalid column"));
        }

        try {
            compiler.compile(factory, "select c.customerId, orderId, o2.productId from " +
                    "customers c " +
                    "join (" +
                    "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                    ") o on c.customerId = o.customerId");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(30, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Invalid column"));
        }
    }

    @Test
    public void testInvalidTableName() throws Exception {
        try {
            assertPlan("", "orders join customer on customerId = customerId");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Journal does not exist"));
        }
    }

    @Test
    public void testJoinCycle() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.customerId\n" +
                        "+ 2[ inner ] d (filter: d.orderId = d.productId) ON d.productId = orders.orderId\n" +
                        "+ 3[ inner ] suppliers ON suppliers.supplier = orders.orderId\n" +
                        "+ 4[ inner ] products ON products.productId = orders.orderId and products.supplier = suppliers.supplier\n" +
                        "\n",
                "orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " where orders.orderId = suppliers.supplier");
    }

    @Test
    public void testJoinGroupBy() throws Exception {
        assertThat("ZHCN\t2.541666666667\n" +
                        "ZEQGMPLUCFTL\t2.549034175334\n" +
                        "ZKX\t2.485995457986\n" +
                        "ZRPFMDVVG\t2.508350730689\n" +
                        "ZV\t2.485329103886\n" +
                        "ZEPIHVLTOVLJUM\t2.485179407176\n" +
                        "ZGKC\t2.525787965616\n" +
                        "ZHEI\t2.464574898785\n" +
                        "ZULIG\t2.471938775510\n" +
                        "ZMZV\t2.470260223048\n" +
                        "ZI\t2.508435582822\n" +
                        "ZYNPPBX\t2.454467353952\n" +
                        "ZEOCVFFKMEKPFOY\t2.414400000000\n",

                "select country, avg(quantity) from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId" +
                        " where country ~ '^Z'");

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
    public void testJoinNoRowid() throws Exception {

        final String expected = "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t104281903\t100\t1138\tKWK\t2015-07-10T00:00:14.518Z\tFBLGGTZEN\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t1191623531\t100\t1210\tSR\t2015-07-10T00:00:18.175Z\tYM\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t1662742408\t100\t1828\tQH\t2015-07-10T00:00:19.509Z\tEYBI\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t220389\t100\t1293\tDGEEWB\t2015-07-10T00:00:56.196Z\tEYBI\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t798408721\t100\t803\tZIHLGS\t2015-07-10T00:00:56.977Z\tVTNNKVOLHLLNN\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t966974434\t100\t870\tJEOBQ\t2015-07-10T00:00:57.981Z\tW\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t258318715\t100\t1036\tOPWOGS\t2015-07-10T00:01:00.608Z\tEYBI\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t1528068156\t100\t400\tYBQE\t2015-07-10T00:01:20.643Z\tQXOLEEXZ\n" +
                "100\tPJFSREKEUNMKWOF\tUVKWCCVTJSKMXVEGPIG\tnull\tVMY\tRT\tEYYPDVRGRQG\t2015-07-10T00:00:00.100Z\t1935884354\t100\t1503\tD\t2015-07-10T00:01:43.507Z\tRZVZJQRNYSRKZSJ\n";

        final RecordSource m = compiler.compileSource(factory, "customers where customerName ~ 'PJFSREKEUNMKWOF'");
        final RecordSource s = new NoRowidSource().of(compiler.compileSource(factory, "orders"));

        RecordSource r = new HashJoinRecordSource(
                m,
                new IntList() {{
                    add(m.getMetadata().getColumnIndex("customerId"));
                }},
                s,
                new IntList() {{
                    add(s.getMetadata().getColumnIndex("customerId"));
                }},
                false
        );
        sink.clear();
        printer.printCursor(r.prepareCursor(factory));
        TestUtils.assertEquals(expected, sink);
        assertThat(expected, "customers c join orders o on c.customerId = o.customerId where customerName ~ 'PJFSREKEUNMKWOF'");
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
                        "+ 1[ inner ] customers ON customers.customerId = orders.orderId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON products.productId = d.productId\n" +
                        "+ 4[ inner ] suppliers ON suppliers.supplier = products.supplier\n" +
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
                        "+ 1[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.customerId\n" +
                        "+ 2[ inner ] customers ON customers.customerId = orders.customerId\n" +
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
    public void testJoinSubQuery() throws Exception {
        assertPlan("+ 0[ cross ] orders\n" +
                        "+ 1[ inner ] {\n" +
                        "  customers (filter: customerName ~ 'X')\n" +
                        "} ON customerName = orderId\n" +
                        "\n",
                "orders" +
                        " cross join (select customerId, customerName from customers where customerName ~ 'X')" +
                        " where orderId = customerName");

    }

    @Test
    public void testJoinWithFilter() throws Exception {
        assertPlan("+ 0[ cross ] customers\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId\n" +
                        "+ 1[ inner ] orders ON orders.orderId = d.orderId (post-filter: d.quantity < orders.orderId)\n" +
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
    public void testLambdaAmbiguousColumn() throws Exception {
        try {
            assertThat("", "orders latest by customerId where customerId in (`select customerName, city from customers where customerName ~ 'PJFSREKEUNMKWOF'`)");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Ambiguous"));
        }
    }

    @Test
    public void testLambdaJoin() throws Exception {
        final String expected = "100\t1935884354\t1503\n";
        assertThat(expected, "select c.customerId, orderId, o.productId from " +
                "customers c " +
                "join (" +
                "orders o latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                ") o on c.customerId = o.customerId");

        assertThat(expected, "select c.customerId, orderId, productId from " +
                "(" +
                "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)" +
                ") o " +
                "join customers c " +
                "on c.customerId = o.customerId");
    }

    @Test
    public void testLatestByAll() throws Exception {
        final String expected = "MUOHOMFQCDD\t1608\tDOT\n" +
                "KUTGTZHMB\t1643\tDPR\n" +
                "PT\t1676\tFVUNIEULONLYL\n" +
                "COX\t1699\tDSVJ\n" +
                "UGSNVUPXB\t1706\tRQRGEVSIPBSFRYM\n" +
                "IIKEBMITV\t1710\tIIIBVNVUJTHWQ\n" +
                "UHSSLJ\t1729\tD\n" +
                "JKNSQ\t1749\tDNSNKZB\n" +
                "REDL\t1767\tCMLIBRH\n" +
                "OH\t1776\tEGUJODLWDVJVX\n" +
                "M\t1783\tQTUR\n" +
                "JSZMYJJIBDSU\t1804\tG\n" +
                "MG\t1807\tYGEHXEXM\n" +
                "YOVV\t1811\tUUIVXRHT\n" +
                "FO\t1821\tMNWSTEFB\n" +
                "XCU\t1825\tWZECVHPTYUR\n" +
                "GIP\t1827\tCUVSRL\n" +
                "MNIKCUJZCRZGKS\t1828\tUOPESHYBD\n" +
                "YCNDZMMOZCGUC\t1838\tIQEZNHNY\n" +
                "MS\t1839\tLQKSGHFNX\n" +
                "TDELODUR\t1854\tEFXWHJNIQID\n" +
                "LKIRSRSPUZBBCEC\t1857\tMIRPINQNRWTCQS\n" +
                "DXDCMGXTPLYG\t1858\tMHGTEG\n" +
                "IDHRPVQMUXXUT\t1863\tOLLLGPDLQXH\n" +
                "ZEMF\t1876\tBGCSFVFWNIHB\n" +
                "DFPWOO\t1883\tEKHB\n" +
                "YYCZINRXDDRNDYI\t1884\tKHB\n" +
                "FUXEJFTGSLMCBRD\t1892\tBMFKMQVMISRBNY\n" +
                "LKXJVOSTCS\t1893\tIONB\n" +
                "GFYMHDPSOJRN\t1894\tBYVNBCLWIHEXQKD\n" +
                "OWBWL\t1899\tRRGTYZCK\n" +
                "VSQCTTQBVGX\t1902\tPDJKHSVTO\n" +
                "IEDGIJKTTTRJ\t1907\tUCVMSJCIECUI\n" +
                "OOUFTRGFOBQUP\t1908\tYL\n" +
                "WPWKGPBPIHSBPJ\t1911\tXGNE\n" +
                "WD\t1912\tDTOXEJHZ\n" +
                "EYSOWXFLJQFTLN\t1914\t\n" +
                "OZEZYTXUQLHDVC\t1915\tUYZZDUILRHK\n" +
                "KXEFLBSJMNF\t1918\tBNCMT\n" +
                "N\t1922\tVPZD\n" +
                "F\t1927\tD\n" +
                "LFQNDNRHKUHE\t1928\tN\n" +
                "WN\t1929\tXFBBIQRKHEBQSTN\n" +
                "TCHHKFDBOXM\t1930\tMGYVMSEPBBB\n" +
                "LPLSXBYJGV\t1935\tWBDRRIS\n" +
                "QZFCS\t1937\tX\n" +
                "THTMCQK\t1939\tCFXMDHZ\n" +
                "TYWWTQU\t1942\tBQQFDLT\n" +
                "JZQXFVV\t1944\tIJDBKIPXC\n" +
                "TBPHWXMBYPD\t1945\t\n" +
                "GROTGCLGILNCXPT\t1946\tEOVGVPEHSZQJGNI\n" +
                "FLDYOSB\t1947\tXIDPTECSRNIULZ\n" +
                "ZZCPO\t1950\tBYYJGDD\n" +
                "\t1951\tXV\n" +
                "LH\t1952\tWLXTV\n" +
                "YETNJDK\t1954\tPKNNWLYVDNKL\n" +
                "GKXFPXPTF\t1955\tPXWIS\n" +
                "BRLOQYMWFYXR\t1956\tYQMHJ\n" +
                "EHGNU\t1957\tMMP\n" +
                "XCGSWYUBPLZIJL\t1958\tZKCCOQKVQYJXWNN\n" +
                "PIBVSPPGQB\t1960\tOKPMW\n" +
                "GEH\t1961\t\n" +
                "FMLZJIGGOB\t1962\tE\n" +
                "EHQCMNSGOLUIYW\t1964\tSQCIDHXLLWYISI\n" +
                "XMCKQC\t1965\tCQ\n" +
                "YNMHE\t1966\tUPO\n" +
                "XSDHPJYW\t1967\tK\n" +
                "BSFFBELM\t1968\tUSEMYWS\n" +
                "TCBNLUQQJENUFQ\t1969\tXRFZVXFB\n" +
                "UQ\t1970\tLLMBHWNPHYKUSO\n" +
                "VWSMJLRTLFSE\t1971\tQBYOW\n" +
                "YEPEDZMXGGPQTII\t1976\tTCIG\n" +
                "LR\t1977\tPGITMNW\n" +
                "IEMVKIGGP\t1978\tGTQBME\n" +
                "OQUNHZBRZUI\t1979\tEQYIBLXWRSNYPIP\n" +
                "FTZVJOBSP\t1980\tBVNQH\n" +
                "RNUP\t1981\tOJXCLDL\n" +
                "TRTVIJESSO\t1982\tNCVTU\n" +
                "TRG\t1983\tTWVBETYKOND\n" +
                "DJSBQG\t1984\tTXOFKCYGYDEFOB\n" +
                "XOIUNEHDUX\t1986\tRRFMGRBSVEPTB\n" +
                "PR\t1988\tBE\n" +
                "IONUQQEHIBISBCY\t1989\tKCDZDQYFLUCTFVB\n" +
                "LKJB\t1991\tSNJ\n" +
                "EWJW\t1992\tQIZYFCPJ\n" +
                "CZGMSTWGT\t1993\tEQIGJH\n" +
                "QNNJITVR\t1994\tGVEMJRFNNF\n" +
                "MTRDCBLINP\t1995\tDOOJRCCWOILYMCF\n" +
                "ZPZTFEXCNCWZU\t1996\tHFXCG\n" +
                "EDXZVBDN\t1997\tUOG\n" +
                "ZZKOWPSRXENWCD\t1998\tGT\n" +
                "XCWMTXCFEOZVK\t1999\tRBVPSFMZUGSTDID\n";

        assertThat(expected, "select supplier, productId, productName from products latest by supplier");
    }

    @Test
    public void testLatestOrderForACustomers() throws Exception {
        assertThat("385427654\t1\t1703\tXEMP\t2015-07-10T00:01:33.250Z\tMZCMZMHGTIQ\n",
                "orders latest by customerId where customerId = 1 and employeeId = 'XEMP'");
    }

    @Test
    public void testLatestOrderForACustomersNoFilter() throws Exception {
        try {
            assertThat("", "orders latest by customerId where employeeId = 'XEMP'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(17, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Filter on int"));
        }
    }

    @Test
    public void testLatestOrderForListOfCustomers() throws Exception {
        assertThat("385427654\t1\t1703\tXEMP\t2015-07-10T00:01:33.250Z\tMZCMZMHGTIQ\n" +
                        "607937606\t2\t548\tOVU\t2015-07-10T00:01:49.101Z\tRZVZJQRNYSRKZSJ\n" +
                        "855411970\t3\t1829\tBRSYPN\t2015-07-10T00:01:51.990Z\tOJXJCNBLYTOIYI\n",
                "orders latest by customerId where customerId in (1,2,3)");
    }

    @Test
    public void testLimit() throws Exception {
        assertThat("1406\tVQHYIIQL\tKJE\tnull\tDYQFLMPNGEJKKJCRCKNPUTHTVNYXM\tDFDISBFBRCCQDV\tXTGNJ\t2015-07-10T00:00:01.406Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                        "1414\tDHMTF\tRTGV\tnull\tHMDJSBGPXQTKPGGWFSTJSKSZSBEPDVNMFEVEMQCOHDBK\tJKBVDSERXZ\tOEENNEBQQEMXD\t2015-07-10T00:00:01.414Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                        "1422\tEKVFCZGGCKCHK\tEEGCRISJP\tnull\tILWBREVXDHHWPREIFCMLRXSWMFWEKIOTXUPRNGEPIJVNKTXHNKYITYG\tYYBJER\tCNSFFLTRY\t2015-07-10T00:00:01.422Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                        "1523\tV\tODSBTX\tnull\tECUZSRJCTRJLH\tVBNHX\tZHEI\t2015-07-10T00:00:01.523Z\tNaN\tNaN\tNaN\t\t\tnull\n" +
                        "1567\tBWPEIFNITZK\tBIJXCYTOHOQT\tnull\tYZMMSEZIPCOCZZUFYIVELTS\tFVPDDGSEK\tUN\t2015-07-10T00:00:01.567Z\tNaN\tNaN\tNaN\t\t\tnull\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId " +
                        " where orderId = NaN" +
                        " limit 10,15");
    }

    @Test
    public void testNullSymbol() throws Exception {
        assertThat("",
                "select country, avg(quantity) from orders o " +
                        "join customers c on c.customerId = o.customerId " +
                        "join orderDetails d on o.orderId = d.orderId" +
                        " where country = null");
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

        assertThat(expected, "customers c" +
                " outer join orders o on c.customerId = o.customerId " +
                " where o.orderId = NaN");
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

    @Test
    public void testRegexConstantTransitivityRhs() throws Exception {
        assertPlan("+ 0[ cross ] c (filter: c.customerId ~ '100')\n" +
                        "+ 1[ outer ] o (filter: o.customerId ~ '100') ON o.customerId = c.customerId\n" +
                        "\n",
                "customers c" +
                        " outer join orders o on c.customerId = o.customerId" +
                        " where c.customerId ~ '100'");
    }

    @Test
    public void testSearchUnIndexedSymbol() throws Exception {
        assertThat("DJSBQG\tDRNUY\tMJSMUTZN\tYHGWBYMZE\tTZ\tNYRZLCBDM\tTEMGDPQYTYUTOPS\t2015-07-10T00:00:12.100Z\n", "suppliers where supplier = 'DJSBQG'");
    }

    @Test
    public void testSimpleIntLambda() throws Exception {
        final String expected = "1935884354\t100\t1503\tD\t2015-07-10T00:01:43.507Z\tRZVZJQRNYSRKZSJ\n";
        assertThat(expected, "orders latest by customerId where customerId in (`customers where customerName ~ 'PJFSREKEUNMKWOF'`)");
        assertThat(expected, "orders latest by customerId where customerId = 100");
    }

    @Test
    public void testSimpleStrToStrLambda() throws Exception {
        assertThat("751505590\t9029\t1516\tFQP\t2015-07-10T00:01:52.290Z\tYGFSMEVDSIYC\n",
                "orders latest by employeeId where employeeId in (`employees where firstName = 'DU'`)");
    }

    @Test
    public void testSimpleStrToSymLambda() throws Exception {
        assertThat("751505590\t9029\t1516\tFQP\t2015-07-10T00:01:52.290Z\tYGFSMEVDSIYC\n",
                "orders latest by employeeId where employeeId in (`select _atos(employeeId) from employees where firstName = 'DU'`)");
    }

    @Test
    public void testSimpleSymToStrLambda() throws Exception {
        assertThat("1946\tEOVGVPEHSZQJGNI\tGROTGCLGILNCXPT\tCOHPFXH\t0.175451457500\t2015-07-10T00:00:14.146Z\n",
                "products latest by supplier where supplier in (`select _stoa(supplier) from suppliers where contactName = 'PHT'`)");
    }

    @Test
    public void testSimpleSymToSymLambda() throws Exception {
        assertThat("1946\tEOVGVPEHSZQJGNI\tGROTGCLGILNCXPT\tCOHPFXH\t0.175451457500\t2015-07-10T00:00:14.146Z\n",
                "products latest by supplier where supplier in (`suppliers where contactName = 'PHT'`)");
    }

    @Test
    public void testThreeWayNoSelect() throws Exception {
        assertThat("customerId\tcustomerName\tcontactName\taddress\tcity\tpostalCode\tcountry\ttimestamp\torderId\tcustomerId\tproductId\temployeeId\torderDate\tshipper\tproductId\tproductName\tsupplier\tcategory\tprice\ttimestamp\n" +
                        "2\tQELQ\tWQGMZBPHETSLOIMSUFXYIWEODDBH\tnull\tVGXYHJUXBWYWRLHUHJECIDLRBIDSTDTFBY\tS\tDCQSCMONRC\t2015-07-10T00:00:00.002Z\t1575627983\t2\t1923\t\t2015-07-10T00:00:31.796Z\tFBLGGTZEN\t1923\tVXZESTU\tLFQNDNRHKUHE\tDDKZCNBOGWTL\t992.000000000000\t2015-07-10T00:00:14.123Z\n" +
                        "2\tQELQ\tWQGMZBPHETSLOIMSUFXYIWEODDBH\tnull\tVGXYHJUXBWYWRLHUHJECIDLRBIDSTDTFBY\tS\tDCQSCMONRC\t2015-07-10T00:00:00.002Z\t1628627044\t2\t1881\tRIXTM\t2015-07-10T00:00:37.249Z\tYGFSMEVDSIYC\t1881\tDJHDQX\tFUXEJFTGSLMCBRD\tKFGXCKKLNVVIQ\t0.000000000000\t2015-07-10T00:00:14.081Z\n" +
                        "2\tQELQ\tWQGMZBPHETSLOIMSUFXYIWEODDBH\tnull\tVGXYHJUXBWYWRLHUHJECIDLRBIDSTDTFBY\tS\tDCQSCMONRC\t2015-07-10T00:00:00.002Z\t541627843\t2\t1216\tUJ\t2015-07-10T00:00:43.578Z\tQPL\t1216\tHZBDMUQLOTHMCHO\tZZCPO\tKSEOSRN\t-234.332031250000\t2015-07-10T00:00:13.416Z\n" +
                        "2\tQELQ\tWQGMZBPHETSLOIMSUFXYIWEODDBH\tnull\tVGXYHJUXBWYWRLHUHJECIDLRBIDSTDTFBY\tS\tDCQSCMONRC\t2015-07-10T00:00:00.002Z\t1502016981\t2\t516\tRK\t2015-07-10T00:00:48.192Z\tOJXJCNBLYTOIYI\t516\tBJUHPV\tUHSSLJ\tQR\t279.675109863281\t2015-07-10T00:00:12.716Z\n" +
                        "2\tQELQ\tWQGMZBPHETSLOIMSUFXYIWEODDBH\tnull\tVGXYHJUXBWYWRLHUHJECIDLRBIDSTDTFBY\tS\tDCQSCMONRC\t2015-07-10T00:00:00.002Z\t1370796605\t2\t1242\t\t2015-07-10T00:00:55.216Z\tYJZPHQDJKOM\t1242\tFLTGLC\tF\tSQDBRUMST\t0.000001773577\t2015-07-10T00:00:13.442Z\n",
                "customers" +
                        " outer join orders o on customers.customerId = o.customerId " +
                        " join products p on o.productId = p.productId" +
                        " limit 25,30", true);
    }

    @Test
    public void testThreeWaySelectAlias() throws Exception {
        assertThat("c.customerId\to.customerId\tp.productId\torderId\n" +
                        "2\t2\t1923\t1575627983\n" +
                        "2\t2\t1881\t1628627044\n" +
                        "2\t2\t1216\t541627843\n" +
                        "2\t2\t516\t1502016981\n" +
                        "2\t2\t1242\t1370796605\n",
                "select c.customerId, o.customerId, p.productId, orderId " +
                        " from customers c" +
                        " outer join orders o on c.customerId = o.customerId " +
                        " join products p on o.productId = p.productId" +
                        " limit 25,30", true);
    }

    @Test
    public void testThreeWaySelectNoAlias() throws Exception {
        assertThat("customers.customerId\to.customerId\tp.productId\torderId\n" +
                        "2\t2\t1923\t1575627983\n" +
                        "2\t2\t1881\t1628627044\n" +
                        "2\t2\t1216\t541627843\n" +
                        "2\t2\t516\t1502016981\n" +
                        "2\t2\t1242\t1370796605\n",
                "select customers.customerId, o.customerId, p.productId, orderId " +
                        " from customers" +
                        " outer join orders o on customers.customerId = o.customerId " +
                        " join products p on o.productId = p.productId" +
                        " limit 25,30", true);
    }

    private static void generateJoinData() throws JournalException, NumericException {
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
                        $sym("category").index().valueCountHint(100).
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
                        $int("customerId").index().
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
                        $sym("supplier").index().valueCountHint(100).
                        $sym("category").index().valueCountHint(100).
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
                        $sym("supplier").valueCountHint(100).
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
}
