/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.ql.model.Statement;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
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
    private final Optimiser optimiser = new Optimiser();
    private final JoinOptimiser joinOptimiser = new JoinOptimiser(optimiser);

    @BeforeClass
    public static void setUp() throws Exception {
        generateJoinData();
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        try {
            parser.setContent("orders join customers on customerId = customerId");
            Statement statement = parser.parse();
            joinOptimiser.compile(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(25, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Ambiguous"));
        }
    }

    @Test
    public void testInvalidAlias() throws Exception {
        try {
            parser.setContent("orders join customers on orders.customerId = c.customerId");
            Statement statement = parser.parse();
            joinOptimiser.compile(statement.getQueryModel(), factory);
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
            joinOptimiser.compile(statement.getQueryModel(), factory);
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
            joinOptimiser.compile(statement.getQueryModel(), factory);
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(12, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Journal does not exist"));
        }
    }

    @Test
    public void testJoinCycle() throws Exception {

        parser.setContent("orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and orders.orderId = products.productId" +
                        " join products on d.productId = products.productId and orders.orderId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where orders.orderId = suppliers.supplier"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders\n" +
                        "+ 3[ inner ] products (filter: products.productId = products.supplier) ON orders.orderId = products.supplier\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "+ 2[ inner ] d (filter: d.orderId = d.productId) ON d.productId = orders.orderId\n" +
                        "+ 1[ inner ] customers ON orders.customerId = customers.customerId\n" +
                        "\n";

        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinImpliedCrosses() throws Exception {
        parser.setContent("orders" +
                        " join customers on 1=1" +
                        " join orderDetails d on 2=2" +
                        " join products on 3=3" +
                        " join suppliers on products.supplier = suppliers.supplier"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 3[ cross ] products\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "+ 0[ cross ] orders\n" +
                        "+ 1[ cross ] customers\n" +
                        "+ 2[ cross ] d\n" +
                        "\n";

        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinMultipleFields() throws Exception {
        parser.setContent("orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders\n" +
                        "+ 1[ inner ] customers ON orders.customerId = customers.customerId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId and d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinOneFieldToTwo() throws Exception {
        parser.setContent("orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 1[ inner ] customers ON orders.orderId = customers.customerId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = customers.customerId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinOneFieldToTwoAcross() throws Exception {
        parser.setContent("orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on orders.orderId = d.orderId and d.orderId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 1[ inner ] customers ON orders.orderId = customers.customerId\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = customers.customerId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinOneFieldToTwoAcross2() throws Exception {
        parser.setContent("orders" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join orderDetails d on d.orderId = customers.customerId and orders.orderId = d.orderId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders (filter: orders.customerId = orders.orderId)\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON orders.orderId = d.orderId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "+ 1[ inner ] customers ON customers.customerId = orders.orderId\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinOneFieldToTwoReorder() throws Exception {
        parser.setContent("orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.orderId = customers.customerId" +
                        " join customers on orders.customerId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders (filter: orders.orderId = orders.customerId)\n" +
                        "+ 2[ inner ] customers ON orders.customerId = customers.customerId\n" +
                        "+ 1[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.customerId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinReorder() throws Exception {
        parser.setContent("orders" +
//                        " join customers on orders.customerId = customers.customerId" +
                        " join customers on 1=1" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
//                        " join orderDetails d on d.orderId = orders.orderId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected = "+ 0[ cross ] orders\n" +
                "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.orderId = orders.orderId\n" +
                "+ 1[ inner ] customers ON d.productId = customers.customerId\n" +
                "+ 3[ inner ] products ON d.productId = products.productId\n" +
                "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinReorderRoot() throws Exception {
        parser.setContent("customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] customers\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId\n" +
                        "+ 1[ inner ] orders ON d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinReorderRoot2() throws Exception {
        parser.setContent("orders" +
//                        " join customers on orders.customerId = customers.customerId" +
                        " outer join customers on 1=1" +
                        " join shippers on shippers.shipper = orders.orderId" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = shippers.shipper" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] orders\n" +
                        "+ 2[ inner ] shippers ON shippers.shipper = orders.orderId\n" +
                        "+ 3[ inner ] d (filter: d.productId = d.orderId) ON d.productId = shippers.shipper and d.orderId = orders.orderId\n" +
                        "+ 4[ inner ] products ON d.productId = products.productId\n" +
                        "+ 5[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "+ 1[ cross ] customers\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
    }

    @Test
    public void testJoinWithFilter() throws Exception {
        parser.setContent("customers" +
                        " cross join orders" +
                        " join orderDetails d on d.orderId = orders.orderId and d.productId = customers.customerId" +
                        " join products on d.productId = products.productId" +
                        " join suppliers on products.supplier = suppliers.supplier" +
                        " where d.productId = d.orderId" +
                        " and (products.price > d.quantity or d.orderId = orders.orderId) and d.quantity < orders.orderId"
        );
        Statement statement = parser.parse();
        joinOptimiser.compile(statement.getQueryModel(), factory);

        final String expected =
                "+ 0[ cross ] customers\n" +
                        "+ 2[ inner ] d (filter: d.productId = d.orderId) ON d.productId = customers.customerId (post-filter: d.quantity < orders.orderId)\n" +
                        "+ 1[ inner ] orders ON d.orderId = orders.orderId\n" +
                        "+ 3[ inner ] products ON d.productId = products.productId (post-filter: products.price > d.quantity or d.orderId = orders.orderId)\n" +
                        "+ 4[ inner ] suppliers ON products.supplier = suppliers.supplier\n" +
                        "\n";
        TestUtils.assertEquals(expected, joinOptimiser.plan());
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

        // statics
        int countryCount = 196;
        ObjList<String> countries = new ObjList<>();
        for (int i = 0; i < countryCount; i++) {
            countries.add(rnd.nextString(rnd.nextInt() & 15));
        }

        // customers
        int customerCount = 10000;
        for (int i = 0; i < customerCount; i++) {
            JournalEntryWriter w = customers.entryWriter();
            w.putInt(0, i);
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(2, rnd.nextChars(rnd.nextInt() & 31));
            w.putStr(4, rnd.nextChars(rnd.nextInt() & 63));
            w.putStr(5, rnd.nextChars(rnd.nextInt() & 15));
            w.putSym(6, countries.getQuick(rnd.nextPositiveInt() % 196));
            w.putDate(7, System.currentTimeMillis());
            w.append();
        }
        customers.commit();

        // categories
        for (int i = 0; i < 100; i++) {
            JournalEntryWriter w = categories.entryWriter();
            w.putSym(0, rnd.nextChars(rnd.nextInt() & 15));
            w.putStr(1, rnd.nextChars(rnd.nextInt() & 63));
            w.putDate(2, System.currentTimeMillis());
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
            w.putDate(4, System.currentTimeMillis());
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
            w.putDate(7, System.currentTimeMillis());
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
            w.putDate(5, System.currentTimeMillis());
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
            int orderId = rnd.nextPositiveInt();

            JournalEntryWriter w = orders.entryWriter(System.currentTimeMillis());
            w.putInt(0, orderId);
            w.putInt(1, rnd.nextPositiveInt() % customerCount);
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
