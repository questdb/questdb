package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Epoll;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public final class InotifyFileWatcher extends FileWatcher {

    private static final Log LOG = LogFactory.getLog(InotifyFileWatcher.class);
    private final long buf;
    private final int bufSize = InotifyAccessor.getSizeofEvent() + 4096;
    private final Path dirPath = new Path();
    private final Epoll epoll = new Epoll(
            new EpollFacadeImpl(),
            2
    );
    private final int fd;
    private final DirectUtf8Sink fileName = new DirectUtf8Sink(0);
    private final int readEndFd;
    private final int wd;
    private final int writeEndFd;

    public InotifyFileWatcher(CharSequence filePath, FileEventCallback callback) throws FileWatcherException {
        super(filePath, callback);

        this.fd = InotifyAccessor.inotifyInit();
        if (this.fd < 0) {
            throw new FileWatcherException("inotify_init");
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
            // todo: add errno
            throw new FileWatcherException("inotify_add_watch");
        }

        if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
            // todo: add errno
            throw new FileWatcherException("epoll_ctl");
        }

        this.buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        if (this.buf < 0) {
            // todo: add errno
            throw new FileWatcherException("malloc");
        }

        long fds = InotifyAccessor.pipe();
        if (fds < 0) {
            throw new FileWatcherException("pipe2");
        }

        this.readEndFd = (int) (fds >>> 32);
        this.writeEndFd = (int) fds;

        if (epoll.control(readEndFd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
            // todo: add errno
            throw new FileWatcherException("epoll_ctl");
        }

    }

    public static void main(String[] args) {
        Os.init();
        int maxChanges = 5;
        AtomicInteger changes = new AtomicInteger(0);
        try (InotifyFileWatcher n = new InotifyFileWatcher(
                "/home/steven/tmp/qdbdev/conf/server.conf",
                changes::incrementAndGet)) {

            n.watch();

            do {
                Thread.sleep(1);
            } while (changes.get() < maxChanges);
        } catch (Throwable thr) {
            thr.printStackTrace();
        }
    }

    @Override
    public void close() {

        if (closed.compareAndSet(false, true)) {
            // Write to pipe to
            if (InotifyAccessor.writePipe(writeEndFd) < 0) {
                // todo: handle error
            }

            super.close();

            this.dirPath.close();
            // todo: close fileName?

            if (InotifyAccessor.inotifyRmWatch(this.fd, this.wd) < 0) {
                System.out.println(this.fd);
            }

            epoll.close();

            Files.close(this.fd);
            Files.close(this.readEndFd);
            Files.close(this.writeEndFd);


            Unsafe.free(this.buf, this.bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);


            LOG.info().$("inotify filewatcher closed").$();
        }


    }

    @Override
    public void waitForChange() throws FileWatcherException {
        // Thread is parked here until epoll is triggered
        if (epoll.poll(-1) < 0) {
            throw new FileWatcherException("epoll_wait");
        }

        if (closed.get()) {
            return;
        }

        // Read the inotify_event into the buffer
        int res = InotifyAccessor.readEvent(fd, buf, bufSize);
        if (res < 0) {
            throw new FileWatcherException("read");
        }

        // iterate over buffer and check all files that have been modified
        int i = 0;
        do {
            int len = Unsafe.getUnsafe().getInt(buf + i + InotifyAccessor.getEventFilenameSizeOffset());
            i += InotifyAccessor.getEventFilenameOffset();
            // I use fileName.size() instead of len because the inotify_event structs have padding. So
            // basically this will match anything server.conf* ... hacky for now just to see if this thing works
            if (Utf8s.equals(fileName, buf + i, fileName.size())) {
                callback.onFileEvent();
                break;
            }
            i += len;
        }
        while (i < res);

        // Rearm the epoll
        if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_MOD, EpollAccessor.EPOLLIN) < 0) {
            throw new FileWatcherException("epoll_ctl (mod)");
        }

    }

}

