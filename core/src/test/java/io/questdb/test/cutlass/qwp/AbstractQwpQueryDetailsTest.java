/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.qwp;

import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared fixture of the tests of the query-details flags of
 * {@code QUERY_REQUEST}: the {@code qd_window} table they page through, and
 * the /exec oracle they check error positions, row counts and JIT use against.
 */
public abstract class AbstractQwpQueryDetailsTest extends AbstractQwpBootstrapTest {
    private static final Pattern EXEC_COUNT = Pattern.compile("\"count\":(\\d+)");
    private static final Pattern EXEC_JIT = Pattern.compile("\"jitCompiled\":(true|false)");
    private static final Pattern EXEC_POSITION = Pattern.compile("\"(?:position|errorPos)\":(\\d+)");
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * A WAL table {@code qd_window} of {@code rows} rows with x = 0, 1, 2, ... in
     * timestamp order, one microsecond apart.
     */
    protected static void createWindowTable(TestServerMain serverMain, long rows) {
        createWindowTable(serverMain, rows, true);
    }

    protected static void createWindowTable(TestServerMain serverMain, long rows, boolean isWal) {
        serverMain.execute("CREATE TABLE qd_window (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL"));
        if (rows > 0) {
            serverMain.execute("INSERT INTO qd_window SELECT x - 1, (x - 1)::TIMESTAMP FROM long_sequence(" + rows + ")");
        }
        serverMain.awaitTable("qd_window");
    }

    /**
     * The row count /exec reports for {@code sql} with {@code count=true}.
     */
    protected static long execCount(String sql) throws Exception {
        return Long.parseLong(find(EXEC_COUNT, exec(sql, "&limit=0,1&count=true")));
    }

    /**
     * Whether /exec reports {@code sql}'s filter as JIT-compiled.
     */
    protected static boolean execJitCompiled(String sql) throws Exception {
        return Boolean.parseBoolean(find(EXEC_JIT, exec(sql, "&limit=0,1&explain=true")));
    }

    /**
     * The error position /exec reports for the failing {@code sql}.
     */
    protected static int execPosition(String sql) throws Exception {
        return Integer.parseInt(find(EXEC_POSITION, exec(sql, "")));
    }

    private static String exec(String sql, String params) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + HTTP_PORT + "/exec?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8) + params
                ))
                .timeout(Duration.ofSeconds(60))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private static String find(Pattern pattern, String json) {
        final Matcher m = pattern.matcher(json);
        Assert.assertTrue(pattern + " not found in " + json, m.find());
        return m.group(1);
    }
}
