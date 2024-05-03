package io.questdb;

import io.questdb.std.Os;

public class FileWatcherFactory {

    public static FileWatcher getFileWatcher(CharSequence filePath, FileEventCallback callback) throws FileWatcherException {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueFileWatcher(filePath, callback);
        } else if (Os.isWindows()) {
            throw new FileWatcherException("windows is not supported yet");
        } else {
            return new InotifyFileWatcher(filePath, callback);
        }
    }
}
