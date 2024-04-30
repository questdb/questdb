package io.questdb;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class KqueueFileEventNotifier implements FileEventNotifier {
    private final int bufferSize;
    private final long evtDir;
    private final int dirFd;
    private final long eventList;
    private final long evtFile;
    private final int fileFd;
    private final int kq;
    private boolean closed;

    public KqueueFileEventNotifier(CharSequence filePath) throws FileEventNotifierException {
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

        kq = KqueueAccessor.kqueue();
        if (kq < 0) {
            throw new FileEventNotifierException("kqueue", kq);
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
            throw new FileEventNotifierException("keventRegister (fileEvent)", fileRes);
        }

        int dirRes = KqueueAccessor.keventRegister(
                kq,
                evtDir,
                1
        );
        if (dirRes < 0) {
            throw new FileEventNotifierException("keventRegister (dirEvent)", dirRes);
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
            KqueueAccessor.evtFree(this.evtFile);
            KqueueAccessor.evtFree(this.evtDir);
        }
    }

    @Override
    public void waitForChange(FileEventCallback callback) throws FileEventNotifierException {
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
            throw new FileEventNotifierException("kevent", res);
        }
        callback.onFileEvent();

    }
}
