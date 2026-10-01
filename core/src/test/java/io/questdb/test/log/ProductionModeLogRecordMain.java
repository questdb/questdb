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

package io.questdb.test.log;

import io.questdb.ParanoiaState;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecord;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class ProductionModeLogRecordMain {

    private ProductionModeLogRecordMain() {
    }

    public static void main(String[] args) throws Exception {
        if (ParanoiaState.LOG_PARANOIA_MODE != ParanoiaState.LOG_PARANOIA_MODE_NONE) {
            throw new AssertionError("expected production log paranoia mode");
        }

        final CountDownLatch consumed = new CountDownLatch(3);
        final LogFactory factory = new LogFactory();
        factory.add(new LogWriterConfig(LogLevel.INFO, (ring, sequence, level) -> new LogWriter() {
            @Override
            public void bindProperties(LogFactory factory) {
            }

            @Override
            public boolean run(WorkerContext workerContext) {
                return sequence.consumeAll(ring, this::consume);
            }

            private void consume(LogRecordUtf8Sink ignored) {
                consumed.countDown();
            }
        }));
        factory.bind();
        factory.startThread();

        final Log logger = factory.create("production-abandoned-record-test");
        final LogRecord firstRecord = logger.info();
        firstRecord.$("first abandoned record");
        final LogRecord secondRecord = logger.info();
        secondRecord.$("second abandoned record");
        logger.info().$("complete record").$();

        if (!consumed.await(5, TimeUnit.SECONDS)) {
            System.err.println("log consumer stopped before all three cursors [remaining=" + consumed.getCount() + ']');
            System.exit(2);
        }
        factory.close();
    }
}
