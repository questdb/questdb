package io.questdb.test.std;

import io.questdb.std.Filewatcher;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.Test;

import java.io.File;

public class FilewatcherTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Test
    public void testFilewatcherSetupAndTeardown() throws Exception {

        final File targetFile = temp.newFile();

        try(Path path = new Path()) {
            path.of(targetFile.getAbsolutePath()).$();
            final long addr = Filewatcher.setup(path.ptr());

            Assert.assertNotEquals(0, addr);
            Filewatcher.teardown(addr);
        }
    }

    static {
        Os.init();
    }

}
