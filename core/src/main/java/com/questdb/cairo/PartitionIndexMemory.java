package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.LPSZ;

public class PartitionIndexMemory extends ReadWriteMemory {
    public PartitionIndexMemory(LPSZ name, int columnCount, boolean create, int defaultPageSize) {
        super(name);
        if (create) {
            configurePageSize(0, defaultPageSize, defaultPageSize);
        } else {
            long header = Files.mmap0(fd, Files.PAGE_SIZE, 0, Files.MAP_RO);
            if (header == -1) {
                throw new RuntimeException("cannot mmap");
            }
            try {
                configurePageSize(Unsafe.getUnsafe().getLong(header), defaultPageSize, defaultPageSize);
            } finally {
                Files.munmap0(header, Files.PAGE_SIZE);
            }
        }
    }
}
