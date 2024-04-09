package io.questdb;

import io.questdb.std.str.Path;

import java.io.IOException;

public final class InotifyDirWatcher implements DirWatcher {

    long dirWatcherPtr;
    boolean closed;

    public InotifyDirWatcher(String dirPath) {
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
    public void close() throws IOException {
        closed = true;
        teardown(dirWatcherPtr);

    }

}
