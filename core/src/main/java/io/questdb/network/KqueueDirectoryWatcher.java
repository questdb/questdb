package io.questdb.network;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

public class KqueueDirectoryWatcher implements Closeable {

    private final int kq;
    private final long eventList;
    private final int bufferSize;
    private final int fd;
    private final long event;
    private boolean closed;


    public KqueueDirectoryWatcher(Path dirPath){

        this.fd = Files.openRO(dirPath);
        if (this.fd < 0) {
            throw CairoException.critical(this.fd).put("could not open directory [path=").put(dirPath).put(']');
        }

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

        this.bufferSize = KqueueAccessor.SIZEOF_KEVENT;
        this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

        // Register event with queue
        int res = KqueueAccessor.keventRegister(
                kq,
                event,
                1
        );
        if (res < 0) {
            throw NetworkError.instance(kq, "could not create new event");
        }

    }

    public interface Callback {
        void onDirectoryChanged();
    }


    public void start(Callback callback){

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

            callback.onDirectoryChanged();
        } while (true);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        Files.close(kq);
        Files.close(fd);
        Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        Unsafe.free(this.event, KqueueAccessor.SIZEOF_KEVENT, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
    }

}
