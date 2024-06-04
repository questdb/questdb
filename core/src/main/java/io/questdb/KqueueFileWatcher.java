package io.questdb;

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class KqueueFileWatcher extends FileWatcher {
    private final int bufferSize;
    private final int dirFd;
    private final long eventList;
    private final long evtDir;
    private final long evtFile;
    private final long evtPipe;
    private final int fileFd;
    private final int kq;
    private final int readEndFd;
    private final int writeEndFd;

    public KqueueFileWatcher(CharSequence filePath, FileEventCallback callback) throws FileWatcherNativeException {
        super(filePath, callback);

        try (Path p = new Path()) {
            p.of(filePath).$();
            this.fileFd = Files.openRO(p);
            if (this.fileFd < 0) {
                throw new FileWatcherNativeException("could not open file [path=%s]", filePath);
            }
            this.dirFd = Files.openRO(p.parent().$());
            if (this.dirFd < 0) {
                Files.close(this.fileFd);
                throw new FileWatcherNativeException("could not open file [path=%s]", p.parent());
            }
        }

        try {
            this.evtFile = KqueueAccessor.evtAlloc(
                    this.fileFd,
                    KqueueAccessor.EVFILT_VNODE,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                            KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                            KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                            KqueueAccessor.NOTE_REVOKE,
                    0
            );

            if (this.evtFile == 0) {
                throw new FileWatcherNativeException("malloc error");
            }

            this.evtDir = KqueueAccessor.evtAlloc(
                    this.dirFd,
                    KqueueAccessor.EVFILT_VNODE,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                            KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                            KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                            KqueueAccessor.NOTE_REVOKE,
                    0
            );

            if (this.evtDir == 0) {
                throw new FileWatcherNativeException("malloc error");
            }

            long fds = KqueueAccessor.pipe();
            this.readEndFd = (int) (fds >>> 32);
            this.writeEndFd = (int) fds;
            Files.bumpFileCount(this.readEndFd);
            Files.bumpFileCount(this.writeEndFd);

            this.evtPipe = KqueueAccessor.evtAlloc(
                    this.readEndFd,
                    KqueueAccessor.EVFILT_READ,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    0,
                    0
            );

            kq = KqueueAccessor.kqueue();
            if (kq < 0) {
                throw new FileWatcherNativeException("kqueue");
            }
            Files.bumpFileCount(this.kq);

            this.bufferSize = KqueueAccessor.SIZEOF_KEVENT;
            this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

            // Register events with queue
            int fileRes = KqueueAccessor.keventRegister(
                    kq,
                    evtFile,
                    1
            );
            if (fileRes < 0) {
                throw new FileWatcherNativeException("keventRegister (fileEvent)");
            }

            int dirRes = KqueueAccessor.keventRegister(
                    kq,
                    evtDir,
                    1
            );
            if (dirRes < 0) {
                throw new FileWatcherNativeException("keventRegister (dirEvent)");
            }

            int pipeRes = KqueueAccessor.keventRegister(
                    kq,
                    evtPipe,
                    1
            );
            if (pipeRes < 0) {
                throw new FileWatcherNativeException("keventRegister (pipeEvent)");
            }

        } finally {
            cleanUp();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (KqueueAccessor.writePipe(writeEndFd) < 0) {
                // todo: handle error
            }

            super.close();

            cleanUp();

        }
    }

    private void cleanUp() {
        if (kq > 0) {
            Files.close(kq);
        }

        if (fileFd > 0) {
            Files.close(fileFd);
        }

        if (dirFd > 0) {
            Files.close(dirFd);
        }

        if (readEndFd > 0) {
            Files.close(readEndFd);
        }

        if (writeEndFd > 0) {
            Files.close(writeEndFd);
        }

        if (this.eventList > 0) {
            Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }

        if (this.evtFile > 0) {
            KqueueAccessor.evtFree(this.evtFile);
        }

        if (this.evtDir > 0) {
            KqueueAccessor.evtFree(this.evtDir);
        }

        if (this.evtPipe > 0) {
            KqueueAccessor.evtFree(this.evtPipe);
        }
    }

    @Override
    protected void waitForChange() throws FileWatcherNativeException {
        // Blocks until there is a change in the watched dir
        int res = KqueueAccessor.keventGetBlocking(
                kq,
                eventList,
                1
        );
        if (closed.get()) {
            return;
        }
        if (res < 0) {
            throw new FileWatcherNativeException("error in keventGetBlocking");
        }
        // For now, we don't filter events down to the file,
        // we trigger on every change in the directory
        // todo: implement kqueue file filter
        runnable.run();
    }
}
