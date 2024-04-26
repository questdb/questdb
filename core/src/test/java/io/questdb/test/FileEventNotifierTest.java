package io.questdb.test;

import io.questdb.FileEventCallback;
import io.questdb.FileEventNotifier;
import io.questdb.FileEventNotifierException;
import io.questdb.FileEventNotifierFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class FileEventNotifierTest extends AbstractTest {

    private SOCountDownLatch threadLatch;

    @Test
    public void testFileWatcher() throws Exception {

        final File targetFile = temp.newFile();
        SOCountDownLatch threadLatch = new SOCountDownLatch(1);

        try (final FileEventNotifier dw = FileEventNotifierFactory.getFileWatcher(temp.getRoot().getAbsolutePath())) {
            Assert.assertNotNull(dw);
            FileChangedCallback callback = new FileChangedCallback(threadLatch);

            Thread thread = new Thread(() -> {
                try {
                    dw.waitForChange(callback);

                } catch (FileEventNotifierException exc) {
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
