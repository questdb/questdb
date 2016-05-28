/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LockManager {

    private static final Log LOG = LogFactory.getLog(LockManager.class);
    private static final Map<String, Lock> locks = new ConcurrentHashMap<>();

    private LockManager() {
    }

    public static Lock lockExclusive(File location) {
        String sharedKey = getKey(location, true);
        String exclusiveKey = getKey(location, false);

        if (locks.get(sharedKey) != null || locks.get(exclusiveKey) != null) {
            return null;
        }

        Lock lock = new Lock(location, false);
        locks.put(exclusiveKey, lock);

        lock.incrementRefCount();
        LOG.debug().$("Exclusive lock successful: ").$(lock).$();
        return lock;
    }

    public static Lock lockShared(File location) {
        String sharedKey = getKey(location, true);
        String exclusiveKey = getKey(location, false);

        Lock lock = locks.get(sharedKey);

        if (lock == null) {
            // we have an exclusive lock in our class loader, fail early
            lock = locks.get(exclusiveKey);
            if (lock != null) {
                return null;
            }

            lock = new Lock(location, true);
            locks.put(sharedKey, lock);
        }

        lock.incrementRefCount();
        LOG.debug().$("Shared lock was successful: ").$(lock).$();
        return lock;
    }

    public static void release(Lock lock) {
        if (lock == null) {
            return;
        }

        File loc = lock.getLocation();
        String sharedKey = getKey(loc, true);
        String exclusiveKey = getKey(loc, false);

        Lock storedSharedLock = locks.get(sharedKey);
        if (storedSharedLock == lock) {
            lock.decrementRefCount();
            if (lock.getRefCount() < 1) {
                lock.release();
                locks.remove(sharedKey);
                LOG.debug().$("Shared lock released: ").$(lock).$();
            }
        }

        Lock storedExclusiveLock = locks.get(exclusiveKey);
        if (storedExclusiveLock == lock) {
            lock.decrementRefCount();
            if (lock.getRefCount() < 1) {
                lock.release();
                lock.delete();
                locks.remove(exclusiveKey);
                LOG.debug().$("Exclusive lock released: ").$(lock).$();
            }
        }
    }

    private static String getKey(File location, boolean shared) {
        return (shared ? "ShLck:" : "ExLck:") + location.getAbsolutePath();
    }
}
