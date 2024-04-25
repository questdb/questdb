package io.questdb;

import io.questdb.std.QuietCloseable;

public interface FileWatcher extends QuietCloseable {
    void waitForChange(FileWatcherCallback callback) throws FileWatcherException;
}

