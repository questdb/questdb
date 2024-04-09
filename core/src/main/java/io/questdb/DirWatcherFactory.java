package io.questdb;

import io.questdb.std.Os;
import io.questdb.std.str.Path;

public class DirWatcherFactory {

    public static DirWatcher GetDirWatcher(Path dirPath) {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueDirWatcher(dirPath);
        } else if (Os.isWindows()) {
            throw new IllegalStateException("no DirWatcher windows support");
        } else {
            return new InotifyDirWatcher(dirPath);
        }
    }
}
