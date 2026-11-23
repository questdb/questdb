/*******************************************************************************
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

import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;

import java.util.function.Supplier;

import static io.questdb.PropertyKey.HTTP_MIN_ENABLED;
import static io.questdb.PropertyKey.LINE_TCP_ENABLED;
import static io.questdb.PropertyKey.PG_ENABLED;

/**
 * Base for QWP egress tests that drive the server only through the engine and the QWP WebSocket
 * endpoint. Each test boots its own {@link TestServerMain} inside an {@code assertMemoryLeak} +
 * try-with-resources so per-test FD/native-memory checks apply and a failing test cannot poison
 * the next one with leftover server state.
 * <p>
 * The base class only sets up the on-disk configuration and the fragmentation chunk sizes of
 * each test, used by {@link #startEgressServer()}; the dummy server config skips the PGWire, ILP
 * and HTTP-min listeners that egress tests never touch. Tests that need a different config call
 * {@link #startServerWithRetry(String...)} directly with their own env vars.
 * <p>
 * Subclasses draw their own random choices from {@link #random}, the Rnd that draws the chunks,
 * not from a Rnd of their own. A test then logs one pair of seeds, which {@code -Dfuzz.s0} and
 * {@code -Dfuzz.s1} pass back to replay every draw of a failing run. The override seeds every Rnd
 * alike, so it cannot replay a run that drew from two Rnds, and {@link OneSeedPairCheck} fails a
 * test or a test class that seeds another one. A subclass that needs another Rnd seeds it from
 * {@link #random}, through {@link TestUtils#generateRandom(Log, long, long)}.
 */
public abstract class AbstractReusedServerQwpEgressTest extends AbstractBootstrapTest {

    @ClassRule
    public static final TestRule oneSeedPairClassCheck = OneSeedPairCheck.CLASS_RULE;
    private static final Log LOG = LogFactory.getLog(AbstractReusedServerQwpEgressTest.class);
    @Rule
    public final OneSeedPairCheck oneSeedPairCheck = new OneSeedPairCheck(LOG);
    // draws the chunks, then every random choice the test makes
    protected Rnd random;
    protected int recvChunk;
    protected int sendChunk;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // AbstractTest.tearDown() wipes the root between tests, so the conf/ directory
        // written by createDummyConfiguration() goes with it. Re-create it here so each
        // test's freshly booted server reads our port + listener overrides instead of
        // falling back to default ports.
        try {
            // Egress tests only use the HTTP/QWP WebSocket port; skip the listeners they never touch.
            createDummyConfiguration(
                    PG_ENABLED + "=false",
                    LINE_TCP_ENABLED + "=false",
                    HTTP_MIN_ENABLED + "=false"
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Before
    public void setUpRandom() {
        // A fresh chunk pair for each test, from the Rnd the test draws its data from, so the
        // one pair of seeds a test logs replays both.
        random = oneSeedPairCheck.getRandom();
        recvChunk = 1 + random.nextInt(500);
        sendChunk = 1 + random.nextInt(500);
    }

    /**
     * Starts a {@link QueryAssertion} on the engine and SQL execution context of
     * {@code serverMain}. It skips the leak check: the running server allocates on its own
     * threads, and each test checks for leaks around the whole server instead.
     */
    protected QueryAssertion assertQuery(TestServerMain serverMain, CharSequence query) {
        return assertQuery(query)
                .withEngine(serverMain.getEngine())
                .withContext(serverMain.getSqlExecutionContext())
                .noLeakCheck();
    }

    /**
     * Convenience: boot a server with the fragmentation chunk sizes of the test. Most egress tests
     * use this; tests that need extra env vars call {@link #startServerWithRetry(String...)}
     * directly.
     */
    protected TestServerMain startEgressServer() {
        return startServerWithRetry(
                PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(recvChunk),
                PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(),
                Integer.toString(sendChunk)
        );
    }

    /**
     * Boots a server, retrying on a bind failure. These tests use fixed ports, and closing a server
     * does not release its listening port instantly, so a server booting right after another on the
     * same ports closed can hit EADDRINUSE. Back off and retry until the port frees.
     */
    protected static TestServerMain startServerWithRetry(String... envs) {
        return startServerWithRetry(() -> startWithEnvVariables(envs));
    }

    /**
     * As {@link #startServerWithRetry(String...)}, for a server that {@code start} builds and
     * starts itself. On failure {@code start} must close the half-started server before throwing.
     */
    protected static TestServerMain startServerWithRetry(Supplier<TestServerMain> start) {
        Throwable lastError = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                return start.get();
            } catch (NetworkError | Bootstrap.BootstrapException e) {
                // The lifecycle reports a listener that failed to bind as a startup failure
                // of its component, so the bind error can arrive wrapped in either. start
                // already closed the half-started server before rethrowing.
                if (e.getMessage() == null || !Chars.contains(e.getMessage(), "could not bind socket")) {
                    throw e;
                }
                lastError = e;
                LOG.info().$("server port still in use, retrying boot [attempt=").$(attempt).$(']').$();
                Os.sleep(100);
            }
        }
        if (lastError instanceof Bootstrap.BootstrapException e) {
            throw e;
        }
        throw (NetworkError) lastError;
    }
}
