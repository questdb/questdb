package io.questdb.test;

import io.questdb.*;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class FileChangedCallbackTest  {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Test
    public void TestDirWatcher() throws Exception {

        final File targetFile = temp.newFile();

        try (Path path = new Path()) {
            path.of(targetFile.getAbsolutePath()).$();
            final DirWatcher dw = DirWatcherFactory.GetDirWatcher(path);
            FileChangedCallback callback = new FileChangedCallback(path);

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
                writer.println("hello again ");
            }

            Thread.sleep(10);

            Assert.assertTrue(callback.pollChanged());

            dw.close();


        }


    }

    static {
        Os.init();
    }
}
