package io.questdb.network;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

public class KqueueFilewatcher implements Closeable {

    private final int kq;
    private final long eventList;
    private final int capacity = 1;
    private final int bufferSize;
    private final Path fileToWatch;
    private final Path dir;
    private final int fd;
    private final long event;
    private boolean changed;
    private long lastModified;
    private boolean closed;


    public KqueueFilewatcher(Path filePath){
        this.fileToWatch = new Path(255).of(filePath);
        this.dir = new Path(255).of(this.fileToWatch).parent();
        this.fd = Files.openRO(dir);
        if (this.fd < 0) {
            throw CairoException.critical(this.fd).put("could not open file [path=").put(this.dir).put(']');
        }
        this.lastModified = Files.getLastModified(this.fileToWatch);
        this.event = KqueueAccessor.evSet(
                this.fd,
                KqueueAccessor.EVFILT_VNODE,
                KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                KqueueAccessor.NOTE_WRITE,
                0
        );

        kq = KqueueAccessor.kqueue();
        if (kq < 0) {
            throw NetworkError.instance(kq, "could not create kqueue");
        }
        Files.bumpFileCount(this.kq);

        this.bufferSize = KqueueAccessor.SIZEOF_KEVENT * capacity;
        this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

        // Register event with queue
        int res = KqueueAccessor.keventRegister(
                kq,
                this.event,
                capacity
        );
        if (res < 0) {
            throw NetworkError.instance(kq, "could not create new event");
        }

    }


    public void start(){

        do {
            // Blocks until there is a change in the watched dir
            int res = KqueueAccessor.keventGetBlocking(
                    kq,
                    eventList,
                    1
            );
            if (res < 0) {
                if (closed) {
                    return;
                }
                throw NetworkError.instance(kq, "could not get event");
            };

            long lastModified = Files.getLastModified(this.fileToWatch);
            if (lastModified > this.lastModified) {
                this.changed = true;
                this.lastModified = lastModified;
            }
        } while (true);
    }
    public boolean changed(){
        if (this.changed) {
            this.changed = false;
            return true;
        }
        return false;
    }
    @Override
    public void close() throws IOException {
        closed = true;
        Files.close(kq);
        Files.close(fd);
        Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
    }

}
