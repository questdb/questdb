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

package io.questdb.test.std.filewatch;

import io.questdb.FileEventCallback;
import io.questdb.cairo.CairoException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.std.filewatch.FileWatcher;
import io.questdb.std.filewatch.FileWatcherFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FileWatcherTest extends AbstractTest {

    @Test
    public void testEmptyFilename() throws Exception {
        assertMemoryLeak(() -> Assert.assertThrows(
                IllegalArgumentException.class,
                () -> FileWatcherFactory.getFileWatcher(new Utf8String(""), Assert::fail)
        ));
    }

    @Test
    public void testFileAppend() throws Exception {

        assertMemoryLeak(() -> {
            final File targetFile = temp.newFile();
            SOCountDownLatch threadLatch = new SOCountDownLatch(1);

            try (
                    final FileWatcher fw = FileWatcherFactory.getFileWatcher(
                            new Utf8String(targetFile.getAbsolutePath()),
                            new TestFileEventCallback(threadLatch)
                    )
            ) {
                fw.start();
                Os.sleep(500);
                try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath())) {
                    writer.println("hello");
                }
                threadLatch.await();
            }
        });
    }

    @Test
    public void testFileCopyOnWrite() throws Exception {
        assertMemoryLeak(() -> {
            final File targetFile = temp.newFile();
            SOCountDownLatch threadLatch = new SOCountDownLatch(1);

            try (
                    final FileWatcher fw = FileWatcherFactory.getFileWatcher(
                            new Utf8String(targetFile.getAbsolutePath()),
                            new TestFileEventCallback(threadLatch)
                    )
            ) {
                fw.start();
                Os.sleep(1000);
                Assert.assertTrue(targetFile.delete());
                try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath())) {
                    writer.println("hello");
                }
                threadLatch.await();
            }
        });
    }

    @Test
    public void testFileDoesNotExist() throws Exception {
        assertMemoryLeak(() -> Assert.assertThrows(
                CairoException.class,
                () -> FileWatcherFactory.getFileWatcher(
                        new Utf8String("/hello/i/dont/exist"),
                        Assert::fail
                )
        ));
    }

    private static class TestFileEventCallback implements FileEventCallback {
        SOCountDownLatch latch;

        public TestFileEventCallback(SOCountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onFileEvent() {
            latch.countDown();
        }
    }

    static {
        Os.init();
    }
}
