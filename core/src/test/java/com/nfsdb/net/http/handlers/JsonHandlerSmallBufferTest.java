package com.nfsdb.net.http.handlers;

import com.nfsdb.factory.JournalFactoryPool;
import com.nfsdb.net.http.HttpServer;
import com.nfsdb.net.http.HttpServerConfiguration;
import com.nfsdb.net.http.QueryResponse;
import com.nfsdb.net.http.SimpleUrlMatcher;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import org.apache.http.MalformedChunkCodingException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class JsonHandlerSmallBufferTest extends AbstractOptimiserTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static JournalFactoryPool factoryPool;
    private static HttpServer server;
    private static JsonHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        factoryPool = new JournalFactoryPool(factory.getConfiguration(), 1);
        handler = new JsonHandler(factoryPool);

        HttpServerConfiguration configuration = new HttpServerConfiguration();
        configuration.setHttpBufRespContent(128);
        server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            put("/js", handler);
        }});

        server.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        server.halt();
        factoryPool.close();
    }

    @Test
    public void testJsonChunkOverflow() throws Exception {
        int count = 10000;
        JsonHandlerTest.generateJournal("large", count);
        QueryResponse queryResponse = JsonHandlerTest.download("large", temp);
        Assert.assertEquals(count, queryResponse.result.length);
    }

    @Test(expected = MalformedChunkCodingException.class)
    public void testJsonEncodeControlChars() throws Exception {
        StringBuilder allChars = new StringBuilder();
        for (char c = Character.MIN_VALUE; c < 0xD800; c++) { //
            allChars.append(c);
        }

        String allCharString = allChars.toString();
        JsonHandlerTest.generateJournal("xyz", allCharString, 1.900232E-10, 2.598E20, Long.MAX_VALUE, Integer.MIN_VALUE, -102023);
        String query = "select id from xyz \n limit 1";
        JsonHandlerTest.download(query, temp);
    }
}