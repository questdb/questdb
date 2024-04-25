package io.questdb;

import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkError;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class KqueueFileWatcher implements FileWatcher {
    private final int bufferSize;
    private final long fileEvent;
    private final long dirEvent;
    private final long eventList;
    private final int fileFd;
    private final int dirFd;
    private final int kq;
    private boolean closed;

    public KqueueFileWatcher(CharSequence filePath) throws FileWatcherException {
        try (Path p = new Path()) {
            p.of(filePath).$();
            this.fileFd = Files.openRO(p);
            if (this.fileFd < 0) {
                throw CairoException.critical(this.fileFd).put("could not open file [path=").put(filePath).put(']');
            }

            this.dirFd = Files.openRO(p.parent());
            if (this.dirFd < 0) {
                throw CairoException.critical(this.dirFd).put("could not open dir [path=").put(p.parent()).put(']');
            }
        }

        this.fileEvent = KqueueAccessor.evSet(
                this.fileFd,
                KqueueAccessor.EVFILT_VNODE,
                KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                        KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                        KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                        KqueueAccessor.NOTE_REVOKE,
                0
        );

        this.dirEvent = KqueueAccessor.evSet(
                this.dirFd,
                KqueueAccessor.EVFILT_VNODE,
                KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                KqueueAccessor.NOTE_REVOKE,
                0
        );

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
                fileEvent,
                1
        );
        if (fileRes < 0) {
            throw new FileWatcherException("keventRegister (fileEvent)", fileRes);
        }

        int dirRes = KqueueAccessor.keventRegister(
                kq,
                dirEvent,
                1
        );
        if (dirRes < 0) {
            throw new FileWatcherException("keventRegister (dirEvent)", dirRes);
        }

    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Files.close(kq);
            Files.close(fileFd);
            Files.close(dirFd);
            Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Unsafe.free(this.fileEvent, KqueueAccessor.SIZEOF_KEVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Unsafe.free(this.dirEvent, KqueueAccessor.SIZEOF_KEVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }
    }

    @Override
    public void waitForChange(FileWatcherCallback callback) throws FileWatcherException {
        // Blocks until there is a change in the watched dir
        int res = KqueueAccessor.keventGetBlocking(
                kq,
                eventList,
                1
        );
        if (closed) {
            return;
        }
        if (res < 0) {
            throw new FileWatcherException("kevent", res);
        }
        callback.onFileChanged();

    }
}
