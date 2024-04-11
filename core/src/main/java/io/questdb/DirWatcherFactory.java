package io.questdb;

import io.questdb.std.Os;

public class DirWatcherFactory {

    public static DirWatcher getDirWatcher(CharSequence dirPath) {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueDirWatcher(dirPath);
        } else if (Os.isWindows()) {
            // todo: implement Windows dirWatcher
            return null;
        } else {
            return new InotifyDirWatcher(dirPath);
        }
    }
}
