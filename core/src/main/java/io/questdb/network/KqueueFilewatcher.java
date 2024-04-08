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
    private final int fd;
    private boolean changed;
    private long lastModified;


    public KqueueFilewatcher(String filePath){
        fileToWatch = new Path().of(filePath);
        this.fd = Files.openRO(fileToWatch.parent());
        if (this.fd < 0) {
            throw CairoException.critical(this.fd).put("could not open file [path=").put(fileToWatch.parent()).put(']');
        }
        this.lastModified = Files.getLastModified(this.fileToWatch.$());

        kq = KqueueAccessor.kqueue();
        if (kq < 0) {
            throw NetworkError.instance(kq, "could not create kqueue");
        }
        Files.bumpFileCount(this.kq);

        this.bufferSize = KqueueAccessor.SIZEOF_KEVENT * capacity;
        this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
    }


    public void start(){

        do {
            // Blocks until there is a change in the watched dir
            KqueueAccessor.keventBlocking(
                    kq,
                    this.fd,
                    capacity,
                    eventList,
                    1
            );

            long lastModified = Files.getLastModified(this.fileToWatch.$());
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
        Files.close(kq);
        Files.close(fd);
        fileToWatch.close();
        Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
    }

}
