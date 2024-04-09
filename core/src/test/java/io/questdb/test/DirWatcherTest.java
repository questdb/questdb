package io.questdb.test;

import io.questdb.*;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class DirWatcherTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Test
    public void TestDirWatcher() throws Exception {

        final File targetFile = temp.newFile();

        final DirWatcher dw = DirWatcherFactory.GetDirWatcher(temp.getRoot().getAbsolutePath());
        FileChangedCallback callback = new FileChangedCallback(targetFile.getAbsolutePath());

        Thread thread = new Thread(() -> {
            do {
                dw.waitForChange(callback);
            } while(true);

        });
        thread.start();

        try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
            writer.println("hello");
        }

        Thread.sleep(10);

        Assert.assertTrue(callback.pollChanged());
        Assert.assertFalse(callback.pollChanged());

        try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
            writer.println("hello again");
        }

        Thread.sleep(10);

        Assert.assertTrue(callback.pollChanged());

        dw.close();





    }

    static {
        Os.init();
    }

    class FileChangedCallback implements DirWatcherCallback {

        String fp;
        long lastModified;
        boolean changed;

        public FileChangedCallback(String fp)  {
            this.fp = fp;
            try (Path p = new Path()) {
                p.of(this.fp);
                this.lastModified = Files.getLastModified(p);
            }
        }

        @Override
        public void onDirChanged() {
            try (Path p = new Path()) {
                p.of(this.fp);
                long lastModified = Files.getLastModified(p);
                if (lastModified > this.lastModified) {
                    this.lastModified = lastModified;
                    this.changed = true;
                }
            }
        }

        public boolean pollChanged() {
            if (changed) {
                changed = false;
                return true;
            }
            return false;

        }
    }
}
