package io.questdb;

import io.questdb.std.Os;
import io.questdb.std.str.Path;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InotifyFileEventNotifier implements FileEventNotifier {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Path dirPath;
    private final long fd;
    private final CharSequence filePath;
    private final long wd;


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
    }

    public static void main(String[] args) {
        try (InotifyFileEventNotifier n = new InotifyFileEventNotifier("/home/steven/qdbtmp")) {
            n.waitForChange(() -> System.out.println("hi"));
        } catch (FileEventNotifierException e) {
            System.out.println(e.getMessage());
        }


    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.dirPath.close();
            InotifyAccessor.inotifyRmWatch(this.fd, this.wd);
            InotifyAccessor.closeFd(this.fd);
        }


    }

    @Override
    public void waitForChange(FileEventCallback callback) {
        if (closed.get()) {
            return;
        }
        // do epoll stuff
        // then callback.onFileEvent();
    }

}

