package io.questdb;

import io.questdb.std.str.Path;

public final class InotifyFileWatcher implements FileWatcher {

    private final long filewatcherPtr;
    private boolean closed;

    public InotifyFileWatcher(CharSequence dirPath) {
        try (Path p = new Path()) {
            p.of(dirPath).$();
            this.filewatcherPtr = setup(p.ptr());
        }
    }

    @Override
    public void close() {
        closed = true;
        teardown(filewatcherPtr);
    }

    @Override
    public void waitForChange(FileWatcherCallback callback) throws FileWatcherException {
        long result;

        result = waitForChange(filewatcherPtr);
        if (closed) {
            return;
        }
        if (result < 0) {
            throw new FileWatcherException("inotify read", (int) result);
        }
        callback.onFileChanged();
    }

    static native long setup(long path);

    static native void teardown(long address);

    static native long waitForChange(long address);
}
