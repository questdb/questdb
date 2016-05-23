/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.google.gson.JsonSyntaxException;
import com.questdb.factory.JournalFactoryPool;
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
    private static JournalFactoryPool factoryPool;
    private static HttpServer server;
    private static QueryHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        factoryPool = new JournalFactoryPool(factory.getConfiguration(), 1);
        handler = new QueryHandler(factoryPool, new ServerConfiguration());

        ServerConfiguration configuration = new ServerConfiguration();
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
        Assert.assertEquals(RECORD_COUNT, queryResponse.result.size());
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