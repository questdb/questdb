/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicInteger;

public final class Lock {

    private static final Log LOG = LogFactory.getLog(Lock.class);

    private final AtomicInteger refCount = new AtomicInteger(0);
    private RandomAccessFile file;
    private FileLock lock;
    private File lockName;
    private String location;

    Lock(String location, boolean shared) {

        try {
            this.location = location;
            this.lockName = new File(location + ".lock");
            this.file = new RandomAccessFile(lockName, "rw");
            int i = 0;
            while (true) {
                try {
                    this.lock = file.getChannel().tryLock(i, 1, shared);
                    break;
                } catch (OverlappingFileLockException e) {
                    if (shared) {
                        i++;
                    } else {
                        this.lock = null;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public synchronized boolean isValid() {
        return lock != null && lock.isValid();
    }

    @Override
    public String toString() {
        return "Lock{" +
                "lockName=" + lockName +
                ", isShared=" + (lock == null ? "NULL" : lock.isShared()) +
                ", isValid=" + (lock == null ? "NULL" : lock.isValid()) +
                ", refCount=" + refCount.get() +
                '}';
    }

    void decrementRefCount() {
        refCount.decrementAndGet();
    }

    synchronized void delete() {
        if (!lockName.delete()) {
            LOG.error().$("Could not delete lock: ").$(lockName).$();
//            throw new JournalRuntimeException("Could not delete lock: %s", lockName);
        }
    }

    String getLocation() {
        return location;
    }

    int getRefCount() {
        return refCount.get();
    }

    void incrementRefCount() {
        refCount.incrementAndGet();
    }

    synchronized void release() {
        try {
            if (isValid()) {
                lock.release();
                lock = null;
            }

            if (file != null) {
                file.close();
                file = null;
            }
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }
}
