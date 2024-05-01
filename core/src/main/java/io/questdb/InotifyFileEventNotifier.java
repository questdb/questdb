package io.questdb;

import io.questdb.network.EpollAccessor;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InotifyFileEventNotifier implements FileEventNotifier {

    private final long buf;
    private final int bufSize = InotifyAccessor.getSizeofEvent();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Path dirPath = new Path();
    private final int epfd;
    private final long event;
    private final int fd;
    private final DirectUtf8Sink fileName = new DirectUtf8Sink(0);
    private final int wd;


    public InotifyFileEventNotifier(CharSequence filePath) throws FileEventNotifierException {
        Os.init();
        this.fd = InotifyAccessor.inotifyInit();
        if (this.fd < 0) {
            throw new FileEventNotifierException("inotify_init");
        }

        this.dirPath.of(filePath).parent().$();
        this.fileName.put(Paths.get(filePath.toString()).getFileName().toString());

        this.wd = InotifyAccessor.inotifyAddWatch(
                this.fd,
                this.dirPath.ptr(),
                InotifyAccessor.IN_CREATE | InotifyAccessor.IN_MODIFY |
                        InotifyAccessor.IN_MOVED_TO | InotifyAccessor.IN_CLOSE_WRITE
        );

        if (this.wd < 0) {
            throw new FileEventNotifierException("inotify_add_watch");
        }

        this.event = Unsafe.calloc(EpollAccessor.SIZEOF_EVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

        epfd = EpollAccessor.epollCreate();
        if (epfd < 0) {
            throw new FileEventNotifierException("epollCreate");
        }


        Unsafe.getUnsafe().putInt(event + EpollAccessor.EVENTS_OFFSET, EpollAccessor.EPOLLIN | EpollAccessor.EPOLLET);
        if (EpollAccessor.epollCtl(epfd, EpollAccessor.EPOLL_CTL_ADD, this.fd, event) < 0) {
            throw new FileEventNotifierException("epoll_ctl");
        }

        this.buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

    }


    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.dirPath.close();
            // todo: close fileName?
            InotifyAccessor.inotifyRmWatch(this.fd, this.wd);
            Files.close(this.fd);
            Unsafe.free(this.event, EpollAccessor.SIZEOF_EVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Unsafe.free(this.buf, this.bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }


    }

    @Override
    public void waitForChange(FileEventCallback callback) throws FileEventNotifierException {
        if (closed.get()) {
            return;
        }

        if (EpollAccessor.epollWait(epfd, event, 1, -1) < 0) {
            throw new FileEventNotifierException("epoll_wait");
        }
        // Read the inotify_event into the buffer
        Files.read(fd, buf, bufSize, 0);
        int len = Unsafe.getUnsafe().getInt(buf + InotifyAccessor.getEventFilenameSizeOffset());

        // Compare the filename from the struct with our watched file
        if (Utf8s.equals(fileName, buf + InotifyAccessor.getEventFilenameOffset(), len)) {
            callback.onFileEvent();
        }


    }

}

