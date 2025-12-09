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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentTcpSenderBootstrapTest extends AbstractBootstrapTest {
    private static final int CONCURRENCY_LEVEL = 64;

    @Before
    public void setUp() {
        super.setUp();
        final String confPath = root + Files.SEPARATOR + "conf";
        String file = confPath + Files.SEPARATOR + "auth.txt";
        TestUtils.unchecked(() -> {
            ILP_WORKER_COUNT = CONCURRENCY_LEVEL;
            createDummyConfiguration("line.tcp.auth.db.path=conf/auth.txt");
            try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
                writer.println("testUser1	ec-p-256-sha256	AKfkxOBlqBN8uDfTxu2Oo6iNsOPBnXkEH4gt44tBJKCY	AL7WVjoH-IfeX_CXo5G1xXKp_PqHUrdo3xeRyDuWNbBX");
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                throw new AssertionError("Failed to create auth.txt", e);
            }
        });
        dbPath.parent().$();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ILP_WORKER_COUNT = 1;
    }

    @Test
    public void testConcurrentAuth() throws Exception {
        int nThreads = CONCURRENCY_LEVEL;
        int iterationCount = 10;
        // this test uses more complex bootstrap configuration than most of the other TCP Sender/Receiver tests
        // the goal is to have it as close to the real production QuestDB server as possible.
        // including using the same authentication factories and configuration
        TestUtils.assertMemoryLeak(() -> {
            // run multiple iteration to increase chances of hitting a race condition
            for (int i = 0; i < iterationCount; i++) {
                testConcurrentSenders(nThreads, i + nThreads);
            }
        });
    }

    private static @NotNull Thread getThread(int threadNo, TestServerMain serverMain, AtomicReference<Throwable> error) {
        return new Thread(() -> {
            try (Sender sender = Sender.builder(Sender.Transport.TCP)
                    .address("localhost")
                    .port(serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort())
                    .enableAuth(AbstractLineTcpReceiverTest.AUTH_KEY_ID1)
                    .authToken(AbstractLineTcpReceiverTest.AUTH_TOKEN_KEY1)
                    .build()) {
                sender.table("test" + threadNo).stringColumn("value", "test").atNow();
                sender.flush();
            } catch (Throwable e) {
                error.set(e);
                throw new RuntimeException(e);
            }
        });
    }

    private static void testConcurrentSenders(int nThreads, int startingOffset) throws InterruptedException {
        try (final TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.LINE_TCP_NET_CONNECTION_LIMIT.getEnvVarName(), String.valueOf(nThreads + 1)
        )) {
            serverMain.start();
            AtomicReference<Throwable> error = new AtomicReference<>();
            ObjList<Thread> threads = new ObjList<>();
            for (int i = startingOffset; i < nThreads + startingOffset; i++) {
                Thread th = getThread(i, serverMain, error);
                threads.add(th);
                th.start();
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }

            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }

            for (int i = startingOffset; i < nThreads + startingOffset; i++) {
                while (serverMain.getEngine().getTableTokenIfExists("test" + i) == null) {
                    // intentionally empty
                    Os.sleep(1);
                }
            }
        }
    }
}
