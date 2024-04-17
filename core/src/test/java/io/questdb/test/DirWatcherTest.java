package io.questdb.test;

import io.questdb.DirWatcher;
import io.questdb.DirWatcherCallback;
import io.questdb.DirWatcherException;
import io.questdb.DirWatcherFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class DirWatcherTest extends AbstractTest {

    private SOCountDownLatch threadLatch;

    @Test
    public void testDirWatcher() throws Exception {

        final File targetFile = temp.newFile();
        SOCountDownLatch threadLatch = new SOCountDownLatch(1);

        try (final DirWatcher dw = DirWatcherFactory.getDirWatcher(temp.getRoot().getAbsolutePath())) {
            Assert.assertNotNull(dw);
            FileChangedCallback callback = new FileChangedCallback(threadLatch);

            Thread thread = new Thread(() -> {
                try {
                    dw.waitForChange(callback);

                } catch (DirWatcherException exc) {
                    Assert.fail(exc.getMessage());
                }
            });
            thread.start();

            try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
                writer.println("hello");
            }
            threadLatch.await();
        }
    }

    static class FileChangedCallback implements DirWatcherCallback {

        SOCountDownLatch latch;

        public FileChangedCallback(SOCountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onDirChanged() {
            this.latch.countDown();
        }
    }

    static {
        Os.init();
    }
}
