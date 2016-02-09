/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.misc;

import com.sun.management.OperatingSystemMXBean;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;

@SuppressFBWarnings({"IICU_INCORRECT_INTERNAL_CLASS_USE"})
public final class Os {
    public static final int WINDOWS = 3;
    public static final int _32Bit = -2;
    public static final int type;
    public static final int OSX = 1;
    public static final int LINUX = 2;
    private static final int UNKNOWN = -1;
    private static final OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private Os() {
    }

    public static native int errno();

    public static native int getPid();

    public static long getSystemMemory() {
        return bean.getTotalPhysicalMemorySize();
    }

    public static void init() {
    }

    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    private static void loadLib(String lib) {
        InputStream is = Os.class.getResourceAsStream(lib);
        if (is == null) {
            throw new Error("Internal error: cannot find " + lib + ", broken package?");
        }

        try {
            File tempLib = null;
            try {
                tempLib = File.createTempFile(lib, ".so");
                // copy to tempLib
                try (FileOutputStream out = new FileOutputStream(tempLib)) {
                    byte[] buf = new byte[4096];
                    while (true) {
                        int read = is.read(buf);
                        if (read == -1) {
                            break;
                        }
                        out.write(buf, 0, read);
                    }
                } finally {
                    tempLib.deleteOnExit();
                }
                System.load(tempLib.getAbsolutePath());
            } catch (IOException e) {
                throw new Error("Internal error: cannot unpack " + tempLib, e);
            }
        } finally {
            Misc.free(is);
        }
    }

    static {
        if ("64".equals(System.getProperty("sun.arch.data.model"))) {
            String osName = System.getProperty("os.name");
            if (osName.contains("Linux")) {
                type = LINUX;
                loadLib("/binaries/linux/libnfsdb.so");
            } else if (osName.contains("Mac")) {
                type = OSX;
                loadLib("/binaries/osx/libnfsdb.dylib");
            } else if (osName.contains("Windows")) {
                type = WINDOWS;
                loadLib("/binaries/windows/libnfsdb.dll");
            } else {
                type = UNKNOWN;
            }
        } else {
            type = _32Bit;
        }
    }
}
