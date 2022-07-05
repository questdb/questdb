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

public class IOUringFacadeImpl implements IOUringFacade {

    public static final IOUringFacadeImpl INSTANCE = new IOUringFacadeImpl();
    private static final boolean available;

    @Override
    public boolean isAvailable() {
        return available;
    }

    @Override
    public long create(int capacity) {
        return IOUringAccessor.create(capacity);
    }

    @Override
    public void close(long ptr) {
        IOUringAccessor.close(ptr);
    }

    @Override
    public int submit(long ptr) {
        return IOUringAccessor.submit(ptr);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    static {
        if (Os.type != Os.LINUX_AMD64 && Os.type != Os.LINUX_ARM64) {
            available = false;
        } else {
            String kernelVersion = IOUringAccessor.kernelVersion();
            available = isIOUringAvailable(kernelVersion);
        }
    }

    /**
     * io_uring is available since kernel 5.1.
     */
    static boolean isIOUringAvailable(String kernelVersion) {
        final String[] versionParts = kernelVersion.split("\\.");
        if (versionParts.length < 3) {
            return false;
        }

        int major;
        try {
            major = Numbers.parseInt(versionParts[0]);
        } catch (NumericException e) {
            return false;
        }

        if (major < 5) {
            return false;
        }
        if (major > 5) {
            return true;
        }

        int minor;
        try {
            minor = Numbers.parseInt(versionParts[1]);
        } catch (NumericException e) {
            return false;
        }

        return minor > 0;
    }
}
