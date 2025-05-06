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

package io.questdb.test.config;

import io.questdb.cairo.CairoException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.preferences.PreferencesStore;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.await;
import static org.junit.Assert.fail;

public class PreferencesStoreConcurrentTest extends AbstractCairoTest {

    @Test
    public void testConcurrentWriteRead() throws Exception {
        final int numOfWriterThreads = 30;
        final int numOfWrites = 500;
        final int numOfReaderThreads = 100;
        final int numOfReads = 500;
        assertMemoryLeak(() -> {
            final String config = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
            try (PreferencesStore preferencesStore = new PreferencesStore(engine.getConfiguration())) {
                preferencesStore.init();
                try (DirectUtf8Sink directUtf8Sink = new DirectUtf8Sink(256)) {
                    for (int j = 0; j < numOfWrites; j++) {
                        directUtf8Sink.clear();
                        directUtf8Sink.put(config);
                        preferencesStore.save(directUtf8Sink, PreferencesStore.Mode.OVERWRITE, j);
                    }
                }

                final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();

                final CyclicBarrier start = new CyclicBarrier(numOfWriterThreads + numOfReaderThreads);
                final SOCountDownLatch end = new SOCountDownLatch(numOfWriterThreads + numOfReaderThreads);
                for (int i = 0; i < numOfWriterThreads; i++) {
                    final int threadIndex = i;
                    new Thread(() -> {
                        await(start);
                        try (DirectUtf8Sink directUtf8Sink = new DirectUtf8Sink(256)) {
                            for (int j = 0; j < numOfWrites; j++) {
                                directUtf8Sink.clear();
                                directUtf8Sink.put(config);
                                try {
                                    preferencesStore.save(directUtf8Sink, PreferencesStore.Mode.OVERWRITE, preferencesStore.getVersion());
                                } catch (CairoException e) {
                                    if (!e.isPreferencesOutOfDateError()) {
                                        throw e;
                                    }
                                }
                            }
                        } catch (Throwable th) {
                            errors.put(threadIndex, th);
                        }
                        end.countDown();
                    }).start();
                }

                for (int i = 0; i < numOfReaderThreads; i++) {
                    final int threadIndex = i;
                    new Thread(() -> {
                        await(start);
                        try {
                            final Utf8StringSink sink = new Utf8StringSink();
                            for (int j = 0; j < numOfReads; j++) {
                                sink.clear();
                                sink.putAscii('{');
                                preferencesStore.populateSettings(sink);
                                sink.clear(sink.size() - 1);
                                sink.putAscii('}');
                                assertContains(sink.toString(), ",\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}");
                            }
                        } catch (Throwable th) {
                            errors.put(threadIndex, th);
                        }
                        end.countDown();
                    }).start();
                }

                end.await();

                if (!errors.isEmpty()) {
                    for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                        LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
                    }
                    fail("Error in threads");
                }
            }
        });
    }
}
