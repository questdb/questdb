package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class FileChangedCallback implements DirWatcherCallback {

    Path fp;
    long lastModified;
    boolean changed;

    public FileChangedCallback(Path fp)  {
        this.fp = fp;
        this.lastModified = Files.getLastModified(fp);


    }

    @Override
    public void onDirChanged() {
        long lastModified = Files.getLastModified(fp);
        if (lastModified > this.lastModified) {
            this.lastModified = lastModified;
            this.changed = true;
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
