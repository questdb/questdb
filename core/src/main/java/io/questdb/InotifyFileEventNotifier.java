package io.questdb;

import io.questdb.std.str.Path;

public final class InotifyFileEventNotifier implements FileEventNotifier {

    private final long filewatcherPtr;
    private boolean closed;


    public InotifyFileEventNotifier(CharSequence dirPath) throws FileEventNotifierException {
        try (Path p = new Path()) {
            p.of(dirPath).$();
            this.filewatcherPtr = setup(p.ptr());
            if (this.filewatcherPtr < 0) {
                throw new FileEventNotifierException("error setting up event notifier");
            }

            assert this.filewatcherPtr != 0;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (filewatcherPtr > 0) {
                teardown(filewatcherPtr);
            }
        }
    }

    @Override
    public void waitForChange(FileEventCallback callback) throws FileEventNotifierException {
        long result;

        if (filewatcherPtr < 0) {
            throw new FileEventNotifierException("filewatcher was not successfully set up");
        }

        result = waitForChange(filewatcherPtr);
        if (closed) {
            return;
        }
        if (result < 0) {
            throw new FileEventNotifierException("inotify read", result);
        }
        callback.onFileEvent();
    }

    static native long setup(long path);

    static native void teardown(long address);

    static native long waitForChange(long address);
}
