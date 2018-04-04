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

package com.questdb.net.http.handlers;

import com.google.gson.GsonBuilder;
import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.net.http.HttpServer;
import com.questdb.net.http.QueryResponse;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.std.Files;
import com.questdb.std.NumericException;
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.HttpTestUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URLEncoder;
import java.sql.Timestamp;

public class QueryHandlerTest extends AbstractOptimiserTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static HttpServer server;
    private static QueryHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        final ServerConfiguration serverConfiguration = new ServerConfiguration();
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = serverConfiguration;
        env.configuration.setHttpThreads(1);
        env.factory = FACTORY_CONTAINER.getFactory();

        handler = new QueryHandler(env);

        env.matcher = new SimpleUrlMatcher() {{
            put("/js", handler);
            put("/chk", new ExistenceCheckHandler(env));
            put("/csv", new CsvHandler(env));
        }};


        server = new HttpServer(env);

        server.start();
        generateJournal();
    }

    @AfterClass
    public static void tearDown2() {
        server.halt();
        Assert.assertEquals(0, FACTORY_CONTAINER.getFactory().getBusyReaderCount());
        Assert.assertEquals(0, FACTORY_CONTAINER.getFactory().getBusyWriterCount());
    }

    @Override
    @After
    public void tearDown() {
    }

    @Test
    public void testDDLCsv() throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/csv?query=" + URLEncoder.encode("create table y(a INT)", "UTF-8");
        HttpTestUtils.download(HttpTestUtils.clientBuilder(false), url, f);
        String response = Files.readStringFromFile(f);
        Assert.assertTrue(response.contains("\"ddl\":\"OK\"}"));
    }

    @Test
    public void testDDLError() throws Exception {
        String response = downloadStr("create table x (a xyz, b DOUBLE)", -1, -1, false, false, temp);
        Assert.assertEquals("{\"query\":\"create table x (a xyz, b DOUBLE)\",\"error\":\"Unsupported type\",\"position\":18}", response);
    }

    @Test
    public void testDDLSimple() throws Exception {
        String response = downloadStr("create table x (a INT, b DOUBLE)", -1, -1, false, false, temp);
        Assert.assertEquals("{\"ddl\":\"OK\"}", response);
    }

    @Test
    public void testJournalDoesNotExist() throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/chk?j=tab2";
        HttpTestUtils.download(HttpTestUtils.clientBuilder(false), url, f);
        TestUtils.assertEquals("Does not exist\r\n", Files.readStringFromFile(f));
    }

    @Test
    public void testJournalExist() throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/chk?j=tab";
        HttpTestUtils.download(HttpTestUtils.clientBuilder(false), url, f);
        TestUtils.assertEquals("Exists\r\n", Files.readStringFromFile(f));
    }

    @Test
    public void testJournalExistJson() throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/chk?j=tab&f=json";
        HttpTestUtils.download(HttpTestUtils.clientBuilder(false), url, f);
        TestUtils.assertEquals("{\"status\":\"Exists\"}", Files.readStringFromFile(f));
    }

    @Test
    public void testJsonChunkOverflow() throws Exception {
        int count = 10000;
        generateJournal("large", count);
        QueryResponse queryResponse = download("large");
        Assert.assertEquals(count, queryResponse.dataset.size());
    }

    @Test
    public void testJsonEmpty() throws Exception {
        QueryResponse queryResponse = download("tab", 0, 0);
        Assert.assertEquals(0, queryResponse.dataset.size());
    }

    @Test
    public void testJsonEmpty0() throws Exception {
        QueryResponse queryResponse = download("tab where 1 = 2", 0, 0);
        Assert.assertEquals(0, queryResponse.dataset.size());
    }

    @Test
    public void testJsonEmptyQuery() throws Exception {
        Assert.assertNull(download("", 0, 0).dataset);
    }

    @Test
    public void testJsonEncodeControlChars() throws Exception {
        java.lang.StringBuilder allChars = new java.lang.StringBuilder();
        for (char c = Character.MIN_VALUE; c < 0xD800; c++) { //
            allChars.append(c);
        }

        String allCharString = allChars.toString();
        generateJournal("xyz", allCharString, 1.900232E-10, 2.598E20, Long.MAX_VALUE, Integer.MIN_VALUE, new Timestamp(0));
        String query = "select id from xyz \n limit 1";
        QueryResponse queryResponse = download(query);
        Assert.assertEquals(query, queryResponse.query);
        for (int i = 0; i < allCharString.length(); i++) {
            Assert.assertTrue("result len is less than " + i, i < queryResponse.dataset.get(0)[0].length());
            Assert.assertEquals(i + "", allCharString.charAt(i), queryResponse.dataset.get(0)[0].charAt(i));
        }
    }

    @Test
    public void testJsonEncodeNumbers() throws Exception {
        generateJournal("nums", null, 1.900232E-10, Double.MAX_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE, new Timestamp(10));
        QueryResponse queryResponse = download("nums limit 20");
        Assert.assertEquals("0.0000000002", queryResponse.dataset.get(0)[1]);
        Assert.assertEquals("1.7976931348623157E308", queryResponse.dataset.get(0)[2]);
        Assert.assertEquals("9223372036854775807", queryResponse.dataset.get(0)[3]);
        Assert.assertNull(queryResponse.dataset.get(0)[4]);
        Assert.assertEquals("1970-01-01T00:00:00.010Z", queryResponse.dataset.get(0)[5]);

        Assert.assertEquals("id4", queryResponse.dataset.get(4)[0]);
        Assert.assertNull(queryResponse.dataset.get(4)[2]);
    }

    @Test
    public void testJsonInvertedLimit() throws Exception {
        String query = "tab limit 10";
        QueryResponse queryResponse = download(query, 10, 5);
        Assert.assertEquals(0, queryResponse.dataset.size());
    }

    @Test
    public void testJsonLimits() throws Exception {
        String query = "tab";
        QueryResponse r = download(query, 2, 4);
        Assert.assertEquals(2, r.dataset.size());
        Assert.assertEquals("id2", r.dataset.get(0)[0]);
        Assert.assertEquals("id3", r.dataset.get(1)[0]);
    }

    @Test
    public void testJsonPooling() throws Exception {
        QueryResponse queryResponse1 = download("tab limit 10");
        QueryResponse queryResponse2 = download("tab limit 10");
        QueryResponse queryResponse3 = download("tab limit 10");
        QueryResponse queryResponse4 = download("tab limit 10");

        Assert.assertEquals(10, queryResponse1.dataset.size());
        Assert.assertEquals(10, queryResponse2.dataset.size());
        Assert.assertEquals(10, queryResponse3.dataset.size());
        Assert.assertEquals(10, queryResponse4.dataset.size());

        Assert.assertTrue(handler.getCacheHits() > 0);
        Assert.assertTrue(handler.getCacheMisses() > 0);
    }

    @Test
    public void testJsonSimple() throws Exception {
        QueryResponse queryResponse = download("select 1 z from tab limit 10");
        Assert.assertEquals(10, queryResponse.dataset.size());
    }

    @Test
    public void testJsonSimpleNoMeta() throws Exception {
        QueryResponse queryResponse = download("select 1 z from tab limit 10", 0, 10, true, false, temp);
        Assert.assertEquals(10, queryResponse.dataset.size());
        Assert.assertNull(queryResponse.query);
    }

    @Test
    public void testJsonSimpleNoMetaAndCount() throws Exception {
        QueryResponse queryResponse = download("select 1 z from tab", 0, 10, true, true, temp);
        Assert.assertEquals(10, queryResponse.dataset.size());
        Assert.assertEquals(1000, queryResponse.count);
        Assert.assertNull(queryResponse.query);
    }

    @Test
    public void testJsonTakeLimit() throws Exception {
        QueryResponse queryResponse = download("tab limit 10", 2, -1);
        Assert.assertEquals(2, queryResponse.dataset.size());
    }

    @Test
    public void testOrderByEmpty() throws Exception {
        QueryResponse queryResponse = download("tab where 1 = 2 order by y", 0, 1000);
        Assert.assertEquals(0, queryResponse.dataset.size());
    }

    @Test
    public void testRename() throws Exception {
        generateJournal("tab3", new QueryResponse.Tab[0], 100);
        String query = "tab3 limit 10";
        QueryResponse queryResponse = download(query, 0, 5);
        Assert.assertEquals(5, queryResponse.dataset.size());
        download("rename table tab3 to tab2");
        TestUtils.assertEquals("{\"query\":\"tab3 limit 10\",\"error\":\"Journal does not exist\",\"position\":0}", downloadStr(query, 0, 5, true, false, temp));
    }

    private static void generateJournal(String name, QueryResponse.Tab[] recs, int count) throws JournalException, NumericException {
        try (JournalWriter w = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure(name).
                        $sym("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < count; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putSym(0, recs.length > i ? recs[i].id : "id" + i);
                ew.putDouble(1, recs.length > i ? recs[i].x : rnd.nextDouble());
                if (recs.length > i) {
                    ew.putDouble(2, recs[i].y);
                    ew.putLong(3, recs[i].z);
                } else {
                    if (rnd.nextPositiveInt() % 10 == 0) {
                        ew.putNull(2);
                    } else {
                        ew.putDouble(2, rnd.nextDouble());
                    }
                    if (rnd.nextPositiveInt() % 10 == 0) {
                        ew.putNull(3);
                    } else {
                        ew.putLong(3, rnd.nextLong() % 500);
                    }
                }
                ew.putInt(4, recs.length > i ? recs[i].w : rnd.nextInt() % 500);
                ew.putDate(5, recs.length > i ? recs[i].timestamp.getTime() : t);
                t += 10;
                ew.append();
            }
            w.commit();
        }
    }

    static QueryResponse download(String queryUrl, TemporaryFolder temp) throws Exception {
        return download(queryUrl, -1, -1, false, false, temp);
    }

    static void generateJournal(String name, int count) throws JournalException, NumericException {
        generateJournal(name, new QueryResponse.Tab[0], count);
    }

    static void generateJournal(String name, String id, double x, double y, long z, int w, Timestamp timestamp) throws JournalException, NumericException {
        QueryResponse.Tab record = new QueryResponse.Tab();
        record.id = id;
        record.x = x;
        record.y = y;
        record.z = z;
        record.w = w;
        record.timestamp = timestamp;
        generateJournal(name, new QueryResponse.Tab[]{record}, 1000);
    }

    private static String downloadStr(String queryUrl, int limitFrom, int limitTo, boolean noMeta, boolean count, TemporaryFolder temp) throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/js?query=" + URLEncoder.encode(queryUrl, "UTF-8");
        if (limitFrom >= 0) {
            url += "&limit=" + limitFrom;
        }
        if (limitTo >= 0) {
            url += "," + limitTo;
        }

        if (noMeta) {
            url += "&nm=true";
        }

        if (count) {
            url += "&count=true";
        }

        HttpTestUtils.download(HttpTestUtils.clientBuilder(false), url, f);
        return Files.readStringFromFile(f);
    }

    private static QueryResponse download(String queryUrl, int limitFrom, int limitTo, boolean noMeta, boolean count, TemporaryFolder temp) throws Exception {
        return new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create().fromJson(
                downloadStr(queryUrl, limitFrom, limitTo, noMeta, count, temp),
                QueryResponse.class);
    }

    private static void generateJournal() throws JournalException, NumericException {
        generateJournal("tab", new QueryResponse.Tab[0], 1000);
    }

    private static QueryResponse download(String queryUrl) throws Exception {
        return download(queryUrl, temp);
    }

    private static QueryResponse download(String queryUrl, int limitFrom, int limitTo) throws Exception {
        return download(queryUrl, limitFrom, limitTo, false, false, temp);
    }
}