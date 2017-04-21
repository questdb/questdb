/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.google.gson.JsonSyntaxException;
import com.questdb.BootstrapEnv;
import com.questdb.net.http.HttpServer;
import com.questdb.net.http.QueryResponse;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.ql.parser.AbstractOptimiserTest;
import org.apache.http.MalformedChunkCodingException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.sql.Timestamp;

public class QueryHandlerSmallBufferTest extends AbstractOptimiserTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final int RECORD_COUNT = 10000;
    private static HttpServer server;
    private static QueryHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.configuration.setHttpBufRespContent(128);
        env.factory = FACTORY_CONTAINER.getFactory();

        handler = new QueryHandler(env);

        env.matcher = new SimpleUrlMatcher() {{
            put("/js", handler);
        }};

        server = new HttpServer(env);

        server.start();
        QueryHandlerTest.generateJournal("large", RECORD_COUNT);
    }

    @AfterClass
    public static void tearDown2() throws Exception {
        server.halt();
    }

    @Test(expected = MalformedChunkCodingException.class)
    public void testColumnValueTooLargeForBuffer() throws Exception {
        StringBuilder allChars = new StringBuilder();
        for (char c = Character.MIN_VALUE; c < 0xD800; c++) { //
            allChars.append(c);
        }

        String allCharString = allChars.toString();
        QueryHandlerTest.generateJournal("xyz", allCharString, 4.900232E-10, 2.598E20, Long.MAX_VALUE, Integer.MIN_VALUE, new Timestamp(-102023));
        String query = "select x, id from xyz \n limit 1";
        QueryHandlerTest.download(query, temp);
    }

    @Test
    public void testJsonChunkOverflow() throws Exception {
        QueryResponse queryResponse = QueryHandlerTest.download("large", temp);
        Assert.assertEquals(RECORD_COUNT, queryResponse.dataset.size());
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