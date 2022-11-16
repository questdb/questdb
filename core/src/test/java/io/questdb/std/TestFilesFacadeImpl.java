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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.util.HashMap;

import static io.questdb.std.Files.DT_DIR;

public class TestFilesFacadeImpl extends FilesFacadeImpl {
    public static final TestFilesFacadeImpl INSTANCE = new TestFilesFacadeImpl();
    private final static Log LOG = LogFactory.getLog(TestFilesFacadeImpl.class);
    private final static HashMap<Long, String> openFiles = new HashMap<>();
    private final static CharSequenceIntHashMap roFds = new CharSequenceIntHashMap();
    private final static CharSequenceIntHashMap rwFds = new CharSequenceIntHashMap();

    @Override
    public boolean close(long fd) {
        untrack(fd);
        return super.close(fd);
    }

    @Override
    public boolean closeRemove(long fd, LPSZ path) {
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
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        track(name, rwFds, fd);
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        track(name, rwFds, fd);
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        track(name, roFds, Integer.MAX_VALUE + fd);
        return fd;
    }

    @Override
    public long openRW(LPSZ name, long opts) {
        long fd = super.openRW(name, opts);
        track(name, rwFds, fd);
        return fd;
    }

    @Override
    public boolean remove(LPSZ name) {
        if (checkRemove(name)) {
            return false;
        }
        return super.remove(name);
    }

    @Override
    public int rmdir(Path path) {
        long p = Files.findFirst(path.address());
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
        if (roFds.keyIndex(name) < 0) {
            LOG.info().$("cannot remove open file, open as read only: ").utf8(name).$();
            return true;
        } else if (rwFds.keyIndex(name) < 0) {
            LOG.info().$("cannot remove open file, open as read-write: ").utf8(name).$();
            return true;
        }
        return false;
    }

    private static synchronized void track(LPSZ name, CharSequenceIntHashMap map, long fd) {
        if (fd > -1 && fd != Integer.MAX_VALUE - 1) {
            String nameStr = Chars.toString(name);
            int keyIndex = map.keyIndex(nameStr);
            if (keyIndex < 0) {
                int count = map.valueAt(keyIndex);
                map.putAt(keyIndex, nameStr, count + 1);
            } else {
                map.putAt(keyIndex, nameStr, 1);
            }
            openFiles.put(fd, nameStr);
        }
    }

    private static synchronized int untrack(long fd) {
        int count = 1;
        String fileNameRW = openFiles.get(fd);
        if (fileNameRW != null) {
            int keyIndex = rwFds.keyIndex(fileNameRW);
            if (keyIndex < 0) {
                count = rwFds.valueAt(keyIndex);
                if (count == 1) {
                    rwFds.removeAt(keyIndex);
                } else {
                    rwFds.putAt(keyIndex, fileNameRW, count - 1);
                }
                openFiles.remove(fd);
            }
        } else {
            String fileNameRO = openFiles.get(Integer.MAX_VALUE + fd);
            if (fileNameRO != null) {
                int keyIndex = roFds.keyIndex(fileNameRO);
                if (keyIndex < 0) {
                    count = roFds.valueAt(keyIndex);
                    if (count == 1) {
                        roFds.removeAt(keyIndex);
                    } else {
                        roFds.putAt(keyIndex, fileNameRO, count - 1);
                    }
                }
                openFiles.remove(Integer.MAX_VALUE + fd);
            } else {
                LOG.error().$("file descriptor not found [fd=").$(fd).$(']').$();
            }
        }
        return count - 1;
    }
}
