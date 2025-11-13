/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.Bootstrap;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.http.processors.LineHttpProcessorState;
import io.questdb.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.cutlass.line.http.LineHttpSenderV2;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class LineHttpSenderLoggingTest extends AbstractBootstrapTest {
    private static final LogCapture capture = new LogCapture();
    private static final Class<?>[] guaranteedLoggers = new Class[]{
            LineHttpProcessorState.class
    };

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(guaranteedLoggers);
        super.setUp();
        capture.start();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(guaranteedLoggers);
    }

    @Test
    public void testAuthorizationErrorLogging() throws Exception {
        final TestSecurityContext testSecurityContext = new TestSecurityContext();
        final String errorMessage = "Test authorization error";

        TestUtils.assertMemoryLeak(() -> {
            // set authorization error
            testSecurityContext.setException(CairoException.authorization().put(errorMessage));

            ingestWithError(testSecurityContext, errorMessage);

            // check that error is logged as non-critical error
            assertLogLevel('E');
        });
    }

    @Test
    public void testCriticalErrorLogging() throws Exception {
        final TestSecurityContext testSecurityContext = new TestSecurityContext();
        final String errorMessage = "Test too many open files";

        TestUtils.assertMemoryLeak(() -> {
            // set critical error
            testSecurityContext.setException(CairoException.critical(24).put(errorMessage));

            ingestWithError(testSecurityContext, errorMessage);

            // check that error is logged as critical error
            assertLogLevel('C');
        });
    }

    private static void assertLogLevel(char errorLevel) {
        capture.waitForRegex(errorLevel + " i.q.c.h.p.LineHttpProcessorState \\[[0-9]*\\] could not commit");
    }

    private static @NotNull Bootstrap createBootstrap(TestSecurityContext testSecurityContext) {
        return new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap1) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap1.getRootDirectory(),
                                bootstrap1.loadProperties(),
                                getEnv(),
                                bootstrap1.getLog(),
                                bootstrap1.getBuildInformation(),
                                FilesFacadeImpl.INSTANCE,
                                bootstrap1.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration) {
                                    @Override
                                    public @NotNull SecurityContextFactory getSecurityContextFactory() {
                                        return (principalContext, interfaceId) -> testSecurityContext;
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        );
    }

    private static void ingestWithError(TestSecurityContext testSecurityContext, String errorMessage) {
        final Bootstrap bootstrap = createBootstrap(testSecurityContext);
        try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
            serverMain.start();

            try (AbstractLineHttpSender sender = new LineHttpSenderV2(
                    "localhost",
                    serverMain.getHttpServerPort(),
                    DefaultHttpClientConfiguration.INSTANCE,
                    null,
                    1000,
                    null,
                    null,
                    null,
                    127,
                    0,
                    1_000,
                    0,
                    Long.MAX_VALUE
            )) {
                sender.table("accounts").symbol("balance", "BOOOO").atNow();
                sender.flush();
                fail("Exception expected");
            } catch (Exception ex) {
                TestUtils.assertContains(ex.getMessage(), errorMessage);
            }
        }
    }

    private static final class TestSecurityContext extends AllowAllSecurityContext {
        private RuntimeException ex;

        @Override
        public void authorizeInsert(TableToken tableToken) {
            throw ex;
        }

        void setException(RuntimeException ex) {
            this.ex = ex;
        }
    }
}
