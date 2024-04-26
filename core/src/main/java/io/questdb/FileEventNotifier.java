package io.questdb;

import io.questdb.std.QuietCloseable;

public interface FileEventNotifier extends QuietCloseable {
    void waitForChange(FileEventCallback callback) throws FileEventNotifierException;
}

