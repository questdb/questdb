package io.questdb;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicBoolean;

public class KqueueFileEventNotifier extends FileWatcher {
    private final int bufferSize;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int dirFd;
    private final long eventList;
    private final long evtDir;
    private final long evtFile;
    private final int fileFd;
    private final int kq;
    private final int readEndFd;
    private final int writeEndFd;

    public KqueueFileEventNotifier(CharSequence filePath, FileEventCallback callback) throws FileWatcherException {
        super(filePath, callback);

        try (Path p = new Path()) {

            p.of(filePath).$();
            this.fileFd = Files.openRO(p);
            if (this.fileFd < 0) {
                throw CairoException.critical(this.fileFd).put("could not open file [path=").put(filePath).put(']');
            }

            this.dirFd = Files.openRO(p.parent().$());
            if (this.dirFd < 0) {
                throw CairoException.critical(this.dirFd).put("could not open dir [path=").put(p.parent()).put(']');
            }
        }

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

        long fds = KqueueAccessor.pipe();
        this.readEndFd = (int) (fds >>> 32);
        this.writeEndFd = (int) fds;
        Files.bumpFileCount(this.readEndFd);
        Files.bumpFileCount(this.writeEndFd);

        kq = KqueueAccessor.kqueue();
        if (kq < 0) {
            throw new FileWatcherException("kqueue", kq);
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
            throw new FileWatcherException("keventRegister (fileEvent)", fileRes);
        }

        int dirRes = KqueueAccessor.keventRegister(
                kq,
                evtDir,
                1
        );
        if (dirRes < 0) {
            throw new FileWatcherException("keventRegister (dirEvent)", dirRes);
        }

    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (KqueueAccessor.writePipe(writeEndFd) < 0) {
                // todo: handle error
            }

            super.close();

            Files.close(kq);
            Files.close(fileFd);
            Files.close(dirFd);
            Files.close(readEndFd);
            Files.close(writeEndFd);
            Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            KqueueAccessor.evtFree(this.evtFile);
            KqueueAccessor.evtFree(this.evtDir);
        }
    }


    @Override
    protected void waitForChange() throws FileWatcherException {
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
            throw new FileWatcherException("kevent", res);
        }
        // For now, we don't filter events down to the file,
        // we trigger on every change in the directory
        // todo: implement kqueue file filter
        callback.onFileEvent();
    }
}
