/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.util.HashMap;
import java.util.Map;

import static io.questdb.std.Files.DT_DIR;

public class TestFilesFacadeImpl extends FilesFacadeImpl {
    public static final TestFilesFacadeImpl INSTANCE = new TestFilesFacadeImpl();
    private final static Log LOG = LogFactory.getLog(TestFilesFacadeImpl.class);
    private final static HashMap<Long, String> openFilesFds = new HashMap<>();
    private final static CharSequenceIntHashMap openPaths = new CharSequenceIntHashMap();

    public static synchronized void resetTracking() {
    }

    @Override
    public boolean close(int fd) {
        untrack(fd);
        return super.close(fd);
    }

    @Override
    public boolean closeRemove(int fd, LPSZ path) {
        if (fd > -1) {
            if (untrack(fd) != 0) {
                // Make sure only 1 usage found so that it's safe to remove after close
                return false;
            }
            super.close(fd);
        }
        // Remove without checking that is open using call to super.
        return super.remove(path);
    }

    @Override
    public int openAppend(LPSZ name) {
        int fd = super.openAppend(name);
        track(name, fd);
        return fd;
    }

    @Override
    public int openCleanRW(LPSZ name, long size) {
        int fd = super.openCleanRW(name, size);
        track(name, fd);
        return fd;
    }

    @Override
    public int openRO(LPSZ name) {
        int fd = super.openRO(name);
        track(name, fd);
        return fd;
    }

    @Override
    public int openRW(LPSZ name, long opts) {
        int fd = super.openRW(name, opts);
        track(name, fd);
        return fd;
    }

    @Override
    public boolean remove(LPSZ name) {
        if (checkRemove(name)) {
            return false;
        }
        boolean ok = super.remove(name);
        if (!ok) {
            LOG.info().$("cannot remove file: ").utf8(name).$(", errno:").$(errno()).$();
        }
        return ok;
    }

    @Override
    public int rmdir(Path path) {
        long p = Files.findFirst(path);
        int len = path.length();
        int errno = -1;
        if (p > 0) {
            try {
                do {
                    long lpszName = findName(p);
                    path.trimTo(len).concat(lpszName).$();
                    if (findType(p) == DT_DIR) {
                        if (Files.strcmp(lpszName, "..") || Files.strcmp(lpszName, ".")) {
                            continue;
                        }
                        if ((errno = rmdir(path)) == 0) {
                            continue;
                        }
                    } else {
                        if (remove(path)) {
                            continue;
                        }
                        errno = errno() > 0 ? errno() : 5;
                    }
                    return errno;
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
            }
            if (Files.rmdir(path.trimTo(len).$()) == 0) {
                return 0;
            }
            return Os.errno();
        }
        return errno;
    }

    private static synchronized boolean checkRemove(LPSZ name) {
        if (openPaths.keyIndex(name) < 0) {
            LOG.info().$("cannot remove, file is open: ").utf8(name).$(", fd=").$(getFdByPath(name)).$();
            return true;
        }
        return false;
    }

    private static Long getFdByPath(CharSequence value) {
        for (Map.Entry<Long, String> entry : openFilesFds.entrySet()) {
            if (Chars.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private static synchronized void track(LPSZ name, long fd) {
        if (fd > -1 && fd != Integer.MAX_VALUE - 1) {
            String nameStr = Chars.toString(name);
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
        String fileName = openFilesFds.get(fd);
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
