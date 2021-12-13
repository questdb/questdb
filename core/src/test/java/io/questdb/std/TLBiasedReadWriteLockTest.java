/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
 *
 ******************************************************************************/

package io.questdb.std;

import org.junit.Test;

public class TLBiasedReadWriteLockTest extends AbstractReadWriteLockTest {

    @Test
    public void testSerialReadLock() {
        TLBiasedReadWriteLock lock = new TLBiasedReadWriteLock();
        for (int i = 0; i < 32; i++) {
            lock.readLock().lock();
            lock.readLock().unlock();
        }
    }

    @Test
    public void testSerialWriteLock() {
        TLBiasedReadWriteLock lock = new TLBiasedReadWriteLock();
        for (int i = 0; i < 32; i++) {
            lock.writeLock().lock();
            lock.writeLock().unlock();
        }
    }

    @Test
    public void testHammerLockSingleReaderSingleWriter() throws Exception {
        TLBiasedReadWriteLock lock = new TLBiasedReadWriteLock();
        testHammerLock(lock, 1, 1, 1000);
    }

    @Test
    public void testHammerLockMultipleReaderSingleWriter() throws Exception {
        TLBiasedReadWriteLock lock = new TLBiasedReadWriteLock();
        testHammerLock(lock, 8, 1, 1000);
    }

    @Test
    public void testHammerLockMultipleReaderMultipleWriter() throws Exception {
        TLBiasedReadWriteLock lock = new TLBiasedReadWriteLock();
        testHammerLock(lock, 8, 8, 1000);
    }
}
