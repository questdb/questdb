package io.questdb;

import io.questdb.std.Os;

public class FileEventNotifierFactory {

    public static FileEventNotifier getFileWatcher(CharSequence filePath) throws FileEventNotifierException {
        if (Os.isOSX() || Os.isFreeBSD()) {
            return new KqueueFileEventNotifier(filePath);
        } else if (Os.isWindows()) {
            // todo: implement Windows FileEventNotifier
            return null;
        } else {
            return new InotifyFileEventNotifier(filePath);
        }
    }
}
