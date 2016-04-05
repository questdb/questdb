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

package com.nfsdb.net.http.handlers;

import com.google.gson.JsonSyntaxException;
import com.nfsdb.factory.JournalFactoryPool;
import com.nfsdb.net.http.HttpServer;
import com.nfsdb.net.http.HttpServerConfiguration;
import com.nfsdb.net.http.QueryResponse;
import com.nfsdb.net.http.SimpleUrlMatcher;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import org.apache.http.MalformedChunkCodingException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.sql.Timestamp;

public class QueryHandlerSmallBufferTest extends AbstractOptimiserTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final int RECORD_COUNT = 10000;
    private static JournalFactoryPool factoryPool;
    private static HttpServer server;
    private static QueryHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        factoryPool = new JournalFactoryPool(factory.getConfiguration(), 1);
        handler = new QueryHandler(factoryPool);

        HttpServerConfiguration configuration = new HttpServerConfiguration();
        configuration.setHttpBufRespContent(128);
        server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            put("/js", handler);
        }});

        server.start();
        QueryHandlerTest.generateJournal("large", RECORD_COUNT);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        server.halt();
        factoryPool.close();
    }

    @Test(expected = MalformedChunkCodingException.class)
    public void testColumnValueTooLargeForBuffer() throws Exception {
        StringBuilder allChars = new StringBuilder();
        for (char c = Character.MIN_VALUE; c < 0xD800; c++) { //
            allChars.append(c);
        }

        String allCharString = allChars.toString();
        QueryHandlerTest.generateJournal("xyz", allCharString, 1.900232E-10, 2.598E20, Long.MAX_VALUE, Integer.MIN_VALUE, new Timestamp(-102023));
        String query = "select x, id from xyz \n limit 1";
        QueryHandlerTest.download(query, temp);
    }

    @Test
    public void testJsonChunkOverflow() throws Exception {
        QueryResponse queryResponse = QueryHandlerTest.download("large", temp);
        Assert.assertEquals(RECORD_COUNT, queryResponse.result.length);
    }

    @Test(expected = JsonSyntaxException.class)
    public void testQuerySizeTooLargeForBuffer() throws Exception {
        StringBuilder b1 = new StringBuilder();
        StringBuilder b2 = new StringBuilder();
        StringBuilder b = b1;
        StringBuilder t = b2;
        b.append("large where z = 409");
        for (int i = 0; i < 10000; i++) {
            t.append('(').append(b).append(") where z = 409");
            StringBuilder z;
            z = t;
            t = b;
            b = z;
            t.setLength(0);
        }
        QueryHandlerTest.download(b.toString(), temp);
    }
}