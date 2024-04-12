package io.questdb;

import io.questdb.std.str.Path;

public final class InotifyDirWatcher implements DirWatcher {

    private final long dirWatcherPtr;
    private boolean closed;

    public InotifyDirWatcher(CharSequence dirPath) {
        try (Path p = new Path()) {
            p.of(dirPath).$();
            this.dirWatcherPtr = setup(p.ptr());
        }
    }

    static native long setup(long path);
    static native void teardown(long address);
    static native long waitForChange(long address);


    @Override
    public void waitForChange(DirWatcherCallback callback) throws DirWatcherException {
        long result;

            result = waitForChange(dirWatcherPtr);
            if (closed) {
                return;
            }
            if (result < 0 ){
                throw new DirWatcherException("inotify read", (int)result);
            }
            callback.onDirChanged();
    }


    @Override
    public void close() {
        closed = true;
        teardown(dirWatcherPtr);
    }
}
