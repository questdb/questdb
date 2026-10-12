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

package io.questdb.test.cutlass.http;

import io.questdb.PropertyKey;
import io.questdb.std.Os;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

/**
 * The HTTP poll wait ({@code http.net.poll.block.timeout}) is for QWP connections only and must
 * cost pg-wire nothing when there is none. HTTP and pg-wire share the network pool by default,
 * and a worker waiting in the HTTP poll serves no pg-wire request: with no QWP connection, no
 * worker waits there, so pg-wire latency stays where it is, with no HTTP connection or with an
 * idle plain HTTP one.
 * <p>
 * The requests come 3 ms apart, where a worker that sat out a 10 ms poll on every idle HTTP pass
 * slowed pg-wire about ten times at two workers (median 80 us to about 950 us). The bound leaves
 * room for noise, not for that.
 */
public class HttpPollBlockSharedPoolTest extends AbstractBootstrapTest {
    private static final long GAP_MICROS = 3_000;
    private static final int SAMPLES = 150;

    @Override
    @Before
    public void setUp() {
        Assume.assumeTrue(Os.isLinux());
        super.setUp();
        try {
            createDummyConfiguration();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testNoQwpConnectionCostsPgNothing() throws Exception {
        // ABAB, so that drift on the host does not favour one arm. The timeout is far above the
        // workers' 1 ms nap and 10 ms sleep
        final long[][] medians = new long[2][2];
        final String[] arms = {"0", "1000", "0", "1000"};
        for (int a = 0; a < arms.length; a++) {
            final long[] m = measureArm(arms[a]);
            final long[] acc = medians[a % 2];
            for (int i = 0; i < m.length; i++) {
                acc[i] += m[i];
            }
        }
        final String[] names = {"pg, no http connection", "pg, idle plain http connection"};
        final StringBuilder report = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            report.append(names[i]).append(": block off ").append(medians[0][i] / 2)
                    .append("us, block on ").append(medians[1][i] / 2).append("us\n");
        }
        System.out.print(report);
        for (int i = 0; i < names.length; i++) {
            final long off = medians[0][i] / 2;
            final long on = medians[1][i] / 2;
            Assert.assertTrue("the HTTP poll wait slows " + names[i] + "\n" + report, on <= 3 * off + 300);
        }
    }

    private static long[] measureArm(String blockTimeout) throws Exception {
        final long[] result = new long[2];
        try (TestServerMain ignore = startWithEnvVariables(
                PropertyKey.HTTP_NET_POLL_BLOCK_TIMEOUT.getEnvVarName(), blockTimeout,
                PropertyKey.SHARED_NETWORK_WORKER_COUNT.getEnvVarName(), "2",
                PropertyKey.SHARED_WORKER_COUNT.getEnvVarName(), "2"
        )) {
            try (Connection conn = getConnection("admin", "quest", PG_PORT); Statement st = conn.createStatement()) {
                for (int i = 0; i < 200; i++) {
                    selectOne(st);
                }
                result[0] = pgMedianMicros(st);
                // an idle plain HTTP connection, armed in the poll, not a QWP one
                try (Socket ignored = new Socket("127.0.0.1", HTTP_PORT)) {
                    Os.sleep(50);
                    result[1] = pgMedianMicros(st);
                }
            }
        }
        return result;
    }

    private static long pgMedianMicros(Statement st) throws Exception {
        final long[] lat = new long[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) {
            LockSupport.parkNanos(GAP_MICROS * 1000);
            final long t0 = System.nanoTime();
            selectOne(st);
            lat[i] = (System.nanoTime() - t0) / 1000;
        }
        Arrays.sort(lat);
        return lat[SAMPLES / 2];
    }

    private static void selectOne(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery("SELECT 1")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getLong(1));
        }
    }
}
