/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.logging;

import com.nfsdb.collections.ObjectFactory;
import com.nfsdb.concurrent.MPSequence;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.SCSequence;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LoggerTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    @Ignore
    public void testSimpleFileLogging() throws Exception {
        final int recLen = 4096;
        RingQueue<LogRecordSink> ring = new RingQueue<>(new ObjectFactory<LogRecordSink>() {
            @Override
            public LogRecordSink newInstance() {
                return new LogRecordSink(recLen);
            }
        }, 1024);

        SCSequence wSeq = new SCSequence();
        MPSequence lSeq = new MPSequence(1024);
        wSeq.followedBy(lSeq);
        lSeq.followedBy(wSeq);

        File file = temp.newFile();
        System.out.println(file);
        final LogFileWriter w = new LogFileWriter(ring, wSeq, file.getAbsolutePath());
        final AsyncLogger logger = new AsyncLogger(ring, lSeq, ring, lSeq, ring, lSeq);

        Executor executor = Executors.newCachedThreadPool();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    w.run(null);
                }
            }
        });

        final CyclicBarrier barrier = new CyclicBarrier(2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < 10000; i++) {
                        logger.info()._("thread A ")._(i).$();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < 10000; i++) {
                        logger.info()._("thread B ")._(i).$();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread.sleep(2000);
        System.out.println("ok");
    }
}
