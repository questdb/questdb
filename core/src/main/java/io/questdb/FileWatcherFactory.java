package io.questdb;

import io.questdb.std.Os;

import java.io.File;

public class FileWatcherFactory {

    public static FileWatcher getFileWatcher(CharSequence filePath) throws FileWatcherException {
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
