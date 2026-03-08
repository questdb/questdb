/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Utf8SequenceIntHashMap;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.util.HashMap;
import java.util.Map;

public class TestFilesFacadeImpl extends FilesFacadeImpl {
    public static final TestFilesFacadeImpl INSTANCE = new TestFilesFacadeImpl();
    private final static Log LOG = LogFactory.getLog(TestFilesFacadeImpl.class);
    private final static HashMap<Long, Utf8String> openFilesFds = new HashMap<>();
    private final static Utf8SequenceIntHashMap openPaths = new Utf8SequenceIntHashMap();
    protected long fd = -1;

    public static synchronized void resetTracking() {
    }

    @Override
    public boolean close(long fd) {
        if (fd > -1 && fd == this.fd) {
            this.fd = -1;
        }
        untrack(fd);
        return super.close(fd);
    }

    @Override
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        track(name, fd);
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        track(name, fd);
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        track(name, fd);
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        track(name, fd);
        return fd;
    }

    @Override
    public void remove(LPSZ name) {
        if (checkRemove(name)) {
            throw CairoException.critical(0).put("removing open file [file=").put(name).put(']');
        }
        super.remove(name);
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        return !checkRemove(name) && super.removeQuiet(name);
    }

    private static synchronized boolean checkRemove(LPSZ name) {
        if (openPaths.keyIndex(name) < 0) {
            LOG.info().$("cannot remove, file is open: ").$(name).$(", fd=").$(getFdByPath(name)).$();
            // For reproducing test failures which happen on Windows only while debugging on another OS, change this to
            return false;
//            return true;
        }
        return false;
    }

    private static Long getFdByPath(Utf8Sequence value) {
        for (Map.Entry<Long, Utf8String> entry : openFilesFds.entrySet()) {
            if (Utf8s.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private static synchronized void track(LPSZ name, long fd) {
        if (fd > -1 && fd != Integer.MAX_VALUE - 1) {
            Utf8String nameStr = Utf8String.newInstance(name);
            int keyIndex = openPaths.keyIndex(nameStr);
            if (keyIndex < 0) {
                int count = openPaths.valueAt(keyIndex);
                openPaths.putAt(keyIndex, nameStr, count + 1);
            } else {
                openPaths.putAt(keyIndex, nameStr, 1);
            }
            openFilesFds.put(fd, nameStr);
        }
    }

    private static synchronized int untrack(long fd) {
        int count = 1;
        Utf8String fileName = openFilesFds.get(fd);
        if (fileName != null) {
            int keyIndex = TestFilesFacadeImpl.openPaths.keyIndex(fileName);
            if (keyIndex < 0) {
                count = TestFilesFacadeImpl.openPaths.valueAt(keyIndex);
                if (count == 1) {
                    TestFilesFacadeImpl.openPaths.removeAt(keyIndex);
                } else {
                    TestFilesFacadeImpl.openPaths.putAt(keyIndex, fileName, count - 1);
                }
                openFilesFds.remove(fd);
            }
        }
        return count - 1;
    }
}
