package io.questdb.test.network;

import io.questdb.network.KqueueDirectoryWatcher;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class KqueueDirectoryWatcherTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Test
    public void TestKqueueFileWatcher() throws Exception {


        final File targetFile = temp.newFile();
        /*
        try (Path path = new Path()) {
            path.of(targetFile.getAbsolutePath()).$();
            final KqueueDirectoryWatcher fw = new KqueueDirectoryWatcher(path);
            Assert.assertFalse(fw.changed());

            Thread thread = new Thread(fw::start);
            thread.start();

            try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
                writer.println("hello");
            }

            Thread.sleep(10);

            Assert.assertTrue(fw.changed());
            Assert.assertFalse(fw.changed());

            try (PrintWriter writer = new PrintWriter(targetFile.getAbsolutePath(), StandardCharsets.UTF_8)) {
                writer.println("hello again ");
            }

            Thread.sleep(10);

            Assert.assertTrue(fw.changed());

            fw.close();


        }

         */
    }

    static {
        Os.init();
    }
}
