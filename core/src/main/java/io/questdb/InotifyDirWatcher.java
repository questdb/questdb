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
    public void waitForChange(DirWatcherCallback callback) {
        long result;
        do {
            result = waitForChange(dirWatcherPtr);
            if (result < 0 ){
                if (closed) {
                    return;
                }
                // todo: throw error here
            }
            callback.onDirChanged();
        } while(true);
    }


    @Override
    public void close() {
        closed = true;
        teardown(dirWatcherPtr);
    }
}
