package io.questdb;

import java.io.Closeable;

public interface DirWatcher extends Closeable {
    void waitForChange(DirWatcherCallback callback);
}

