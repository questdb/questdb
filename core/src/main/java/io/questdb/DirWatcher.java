package io.questdb;

import io.questdb.std.QuietCloseable;

public interface DirWatcher extends QuietCloseable {
    void waitForChange(DirWatcherCallback callback);
}

