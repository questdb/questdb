package io.questdb.test;

import io.questdb.FileEventCallback;
import io.questdb.FileWatcher;
import io.questdb.FileWatcherFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class FileWatcherTest extends AbstractTest {

    private SOCountDownLatch threadLatch;

    @Test
    @Ignore
    public void testFileWatcher() throws Exception {

        final File targetFile = temp.newFile();
        SOCountDownLatch threadLatch = new SOCountDownLatch(1);

        try (final FileWatcher fw = FileWatcherFactory.getFileWatcher(
                temp.getRoot().getAbsolutePath(),
                new FileChangedCallback(threadLatch))) {

            fw.watch();

            // todo: figure out how to wait until the watch is ready...
            Assert.fail("figure out how to wait until the watch is ready...");

            try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
                writer.println("hello");
            }
            threadLatch.await();
        }
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
