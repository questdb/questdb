package io.questdb;

import io.questdb.std.Os;

public class FileWatcherFactory {

    public static FileWatcher getFileWatcher(CharSequence filePath, FileEventCallback callback) throws FileWatcherNativeException {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueFileWatcher(filePath, callback);
        } else if (Os.isWindows()) {
            return null;
        } else {
            return new InotifyFileWatcher(filePath, callback);
        }
    }
}
