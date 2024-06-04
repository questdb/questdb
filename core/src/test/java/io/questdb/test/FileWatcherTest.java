package io.questdb.test;

import io.questdb.FileEventCallback;
import io.questdb.FileWatcher;
import io.questdb.FileWatcherFactory;
import io.questdb.FileWatcherNativeException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FileWatcherTest extends AbstractTest {

    @Test
    public void testEmptyFilename() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertThrows(
                    IllegalArgumentException.class,
                    () -> FileWatcherFactory.getFileWatcher("", Assert::fail)
            );
        });
    }

    @Test
    public void testFileAppend() throws Exception {

        assertMemoryLeak(() -> {
            final File targetFile = temp.newFile();
            SOCountDownLatch threadLatch = new SOCountDownLatch(1);

            try (final FileWatcher fw = FileWatcherFactory.getFileWatcher(
                    targetFile.toString(),
                    new FileChangedCallback(threadLatch))) {

                fw.watch();
                // todo: synchronize the start of the watch here, so we don't write before the watch is set up
                Thread.sleep(1000);
                try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
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

            try (final FileWatcher fw = FileWatcherFactory.getFileWatcher(
                    targetFile.toString(),
                    new FileChangedCallback(threadLatch))) {

                fw.watch();
                // todo: synchronize the start of the watch here, so we don't write before the watch is set up
                Thread.sleep(1000);
                Assert.assertTrue(targetFile.delete());
                try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
                    writer.println("hello");
                }
                threadLatch.await();
            }
        });

    }

    @Test
    public void testFileDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertThrows(
                    FileWatcherNativeException.class,
                    () -> FileWatcherFactory.getFileWatcher(
                            "/hello/i/dont/exist",
                            Assert::fail)
            );
        });

    }

    static class FileChangedCallback implements FileEventCallback {

        SOCountDownLatch latch;

        public FileChangedCallback(SOCountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onFileEvent() {
            this.latch.countDown();
        }
    }

    static {
        Os.init();
    }
}
