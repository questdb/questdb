/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public class IOURingFacadeImpl implements IOURingFacade {

    public static final IOURingFacadeImpl INSTANCE = new IOURingFacadeImpl();
    private static final boolean available;

    /**
     * io_uring is available since kernel 5.1, but we require 5.12 to avoid ulimit -l issues.
     */
    public static boolean isAvailableOn(String kernelVersion) {
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

        return minor > 11;
    }

    @Override
    public void close(long ptr) {
        IOUringAccessor.close(ptr);
    }

    @Override
    public long create(int capacity) {
        return IOUringAccessor.create(capacity);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    @Override
    public IOURing newInstance(int capacity) {
        return new IOURingImpl(this, capacity);
    }

    @Override
    public int submit(long ptr) {
        return IOUringAccessor.submit(ptr);
    }

    @Override
    public int submitAndWait(long ptr, int waitNr) {
        return IOUringAccessor.submitAndWait(ptr, waitNr);
    }

    static {
        if (Os.type != Os.LINUX) {
            available = false;
        } else {
            String kernelVersion = IOUringAccessor.kernelVersion();
            boolean usable = isAvailableOn(kernelVersion);

            // Kernel version check might indicate io_uring *should* be available, but it's not a guarantee.
            // The kernel could be compiled without io_uring support (CONFIG_IO_URING=n),
            // or the user may lack necessary permissions, which is common in containerized or restricted environments.
            // The only reliable way to confirm availability is to try to initialize a ring.
            if (usable) {
                long ioUring = IOUringAccessor.create(8);
                if (ioUring <= 0) {
                    usable = false;
                } else {
                    IOUringAccessor.close(ioUring);
                }
            }
            available = usable;
        }
    }
}
