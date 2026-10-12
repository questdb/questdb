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

package io.questdb.test.cutlass.http.line;

import io.questdb.client.Sender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real-server coverage for {@code httpTokenProvider}, the ingestion half of the OIDC device-flow work in
 * the pinned {@code java-questdb-client} submodule.
 * <p>
 * The client's own token-provider tests all drive a mock HTTP server, which proves how the client builds a
 * request but not that a provider-sourced token survives a real server's HTTP stack and actually lands rows.
 * That gap matters here specifically: this repository's contribution to the device-flow change is the
 * submodule bump, so the assertion that belongs on this side is that the bumped client still ingests.
 * <p>
 * What this can and cannot cover: OIDC token VALIDATION is an Enterprise feature - open-source QuestDB has
 * no {@code acl.oidc.*} and no bearer-token ACL - so an open-source server accepts the request whatever the
 * Authorization header says, and no assertion here can prove the server honoured the credential. Proving
 * that needs an Enterprise server and belongs in the Enterprise tandem. What is provable here is the part
 * that is genuinely open-source client behaviour: a rotating provider is queried per request rather than
 * captured once, and every row still arrives.
 */
public class LineHttpSenderTokenProviderServerTest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testRotatingTokenProviderIngestsAgainstARealServer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                // a provider that hands out a DIFFERENT token every time it is asked. A sender that captured
                // one token at build time would still ingest happily here, so the call count below is what
                // separates the two.
                AtomicInteger tokenCalls = new AtomicInteger();
                final int flushes = 5;
                final int rowsPerFlush = 10;

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .httpTokenProvider(() -> "ROTATING-" + tokenCalls.incrementAndGet())
                        .disableAutoFlush()
                        .build()) {
                    for (int f = 0; f < flushes; f++) {
                        for (int i = 0; i < rowsPerFlush; i++) {
                            sender.table("tokentab")
                                    .symbol("tag", "value" + i % 3)
                                    .longColumn("v", (long) f * rowsPerFlush + i)
                                    .atNow();
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("tokentab");
                serverMain.assertSql("select count() from tokentab", "count\n"
                        + (flushes * rowsPerFlush) + "\n");
                // one pull per flush: the token reaches the wire per request, so a long-lived sender follows
                // rotation instead of going stale on a token captured at build time
                Assert.assertTrue("the provider must be re-queried per request, got " + tokenCalls.get()
                        + " pulls for " + flushes + " flushes", tokenCalls.get() >= flushes);
            }
        });
    }

    @Test
    public void testRowsAfterAProviderFailureStillParseServerSide() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                // The documented contract for a provider that cannot supply a token yet - the shape
                // OidcDeviceAuth::getToken has before signIn() - is that build() succeeds, the deferred pull
                // surfaces the provider's own error at the FIRST ROW, and the stamp stays pending so a later
                // row retries it. The client's own tests stop at "the buffer is non-empty" after recovery.
                // What only a real server can add is that the request built around that failed stamp is not
                // corrupt: the bytes still parse and the rows still land.
                AtomicBoolean signedIn = new AtomicBoolean(false);
                AtomicInteger tokenCalls = new AtomicInteger();
                final int rows = 20;

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .httpTokenProvider(() -> {
                            tokenCalls.incrementAndGet();
                            if (!signedIn.get()) {
                                throw new RuntimeException("no token has been obtained yet");
                            }
                            return "TOKEN-" + tokenCalls.get();
                        })
                        .disableAutoFlush()
                        .build()) {
                    try {
                        sender.table("pendingtab").longColumn("v", -1L).atNow();
                        Assert.fail("expected the not-yet-signed-in provider to fail the first row");
                    } catch (Exception e) {
                        Assert.assertTrue(String.valueOf(e.getMessage()),
                                String.valueOf(e.getMessage()).contains("no token has been obtained yet"));
                    }

                    signedIn.set(true);
                    for (int i = 0; i < rows; i++) {
                        sender.table("pendingtab").longColumn("v", i).atNow();
                    }
                    sender.flush();
                }

                serverMain.awaitTable("pendingtab");
                // exactly the rows written after recovery: the failed row contributed nothing, and it left no
                // partial line behind that would have corrupted the ones that followed
                serverMain.assertSql("select count() from pendingtab", "count\n" + rows + "\n");
                serverMain.assertSql("select min(v), max(v) from pendingtab", "min\tmax\n0\t" + (rows - 1) + "\n");
            }
        });
    }
}
