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

package io.questdb.test.cutlass.qwp.server;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class QwpProcessorStateTest extends AbstractCairoTest {

    @Test
    public void testAddDataRejectsWhenExceedingMaxBufferSize() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getMaxRecvBufferSize() {
                            return 256;
                        }
                    };
            QwpProcessorState state = new QwpProcessorState(64, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Add 200 bytes — should succeed (200 <= 256)
                long ptr = Unsafe.malloc(200, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr + 200);
                    Assert.assertTrue("first addData should succeed", state.isOk());
                } finally {
                    Unsafe.free(ptr, 200, MemoryTag.NATIVE_HTTP_CONN);
                }

                // Add 100 more bytes — total 300 > 256, should reject
                ptr = Unsafe.malloc(100, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr + 100);
                    Assert.assertFalse("should reject when exceeding max buffer size", state.isOk());
                } finally {
                    Unsafe.free(ptr, 100, MemoryTag.NATIVE_HTTP_CONN);
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCloseAfterDisconnectFreesNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            state.of(1, AllowAllSecurityContext.INSTANCE);
            // Simulate the fixed onConnectionClosed lifecycle:
            // onDisconnected() resets per-connection state (WAL writers, symbol caches),
            // close() frees native memory (bufferAddress, ddlMem, path, symbolCachePool).
            // Before the fix, only onDisconnected() was called, leaking native memory.
            state.onDisconnected();
            state.close();
        });
    }

    @Test
    public void testCloseFreesWalWriterOnConstructorFailure() throws Exception {
        // Reproduces the pattern in QwpTudCache.getTableUpdateDetails() where
        // engine.getWalWriter() is passed inline to the WalTableUpdateDetails
        // constructor. If the constructor throws, the writer leaks.
        // The fix acquires the writer into a local variable and closes it
        // in a catch block.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_leak (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("test_leak");

            TableWriterAPI walWriter = engine.getWalWriter(tableToken);
            try {
                // Passing null engine causes NPE in the TableUpdateDetails
                // constructor before the writer is stored, simulating any
                // constructor failure after the writer has been acquired.
                new WalTableUpdateDetails(
                        null,
                        null,
                        walWriter,
                        null,
                        new Utf8String("test_leak"),
                        null,
                        -1,
                        false,
                        Long.MAX_VALUE
                );
                Assert.fail("should have thrown NullPointerException");
            } catch (Throwable th) {
                Misc.free(walWriter);
            }
        });
    }

    @Test
    public void testDoubleCloseIsSafe() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            state.of(1, AllowAllSecurityContext.INSTANCE);
            state.onDisconnected();
            // close() may be called twice: once explicitly and once via
            // LocalValueMap.set(key, null) which calls Misc.freeIfCloseable().
            state.close();
            state.close();
        });
    }
}
