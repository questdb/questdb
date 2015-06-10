/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package com.nfsdb.storage;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.logging.Logger;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LockManager {

    private static final Logger LOGGER = Logger.getLogger(LockManager.class);
    private static final Map<String, Lock> locks = new ConcurrentHashMap<>();

    private LockManager() {
    }

    public static Lock lockExclusive(File location) throws JournalException {
        String sharedKey = getKey(location, true);
        String exclusiveKey = getKey(location, false);

        if (locks.get(sharedKey) != null || locks.get(exclusiveKey) != null) {
            return null;
        }

        Lock lock = new Lock(location, false);
        locks.put(exclusiveKey, lock);

        lock.incrementRefCount();
        LOGGER.trace("Exclusive lock successful: %s", lock);
        return lock;
    }

    public static Lock lockShared(File location) throws JournalException {
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
        LOGGER.trace("Shared lock was successful: %s", lock);
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
                LOGGER.trace("Shared lock released: %s", lock);
            }
        }

        Lock storedExclusiveLock = locks.get(exclusiveKey);
        if (storedExclusiveLock == lock) {
            lock.decrementRefCount();
            if (lock.getRefCount() < 1) {
                lock.release();
                lock.delete();
                locks.remove(exclusiveKey);
                LOGGER.trace("Exclusive lock released: %s", lock);
            }
        }
    }

    private static String getKey(File location, boolean shared) {
        return (shared ? "ShLck:" : "ExLck:") + location.getAbsolutePath();
    }
}
