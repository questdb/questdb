package io.questdb;

import io.questdb.network.EpollAccessor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InotifyFileEventNotifier implements FileEventNotifier {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Path dirPath;
    private final int epfd;
    private final long event;
    private final int fd;
    private final CharSequence filePath;
    private final int wd;


    public InotifyFileEventNotifier(CharSequence filePath) throws FileEventNotifierException {
        Os.init();
        this.fd = InotifyAccessor.inotifyInit();
        if (this.fd < 0) {
            throw new FileEventNotifierException("inotify_init");
        }

        this.filePath = filePath;
        this.dirPath = new Path();
        this.dirPath.of(Paths.get(filePath.toString()).getParent().toString()).$();

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


        Unsafe.getUnsafe().putInt(event + EpollAccessor.EVENTS_OFFSET, EpollAccessor.EPOLLIN | EpollAccessor.EPOLLET | EpollAccessor.EPOLLONESHOT);
        if (EpollAccessor.epollCtl(epfd, EpollAccessor.EPOLL_CTL_ADD, this.fd, event) < 0) {
            throw new FileEventNotifierException("epoll_ctl");
        }

    }


    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.dirPath.close();
            InotifyAccessor.inotifyRmWatch(this.fd, this.wd);
            InotifyAccessor.closeFd(this.fd);
            Unsafe.free(this.event, EpollAccessor.SIZEOF_EVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
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
        callback.onFileEvent();
        if (EpollAccessor.epollCtl(epfd, EpollAccessor.EPOLL_CTL_MOD, this.fd, event) < 0) {
            throw new FileEventNotifierException("epoll_ctl");
        }


    }

}

