package io.questdb;

import io.questdb.std.Os;

public class FileWatcherFactory {

    public static FileWatcher getFileWatcher(CharSequence filePath) {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueFileWatcher(filePath);
        } else if (Os.isWindows()) {
            // todo: implement Windows dirWatcher
            return null;
        } else {
            return new InotifyFileWatcher(filePath);
        }
    }
}
