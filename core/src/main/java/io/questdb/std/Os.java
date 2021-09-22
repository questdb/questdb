/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.ex.FatalError;
import io.questdb.std.ex.KerberosException;
import io.questdb.std.str.CharSequenceZ;
import io.questdb.std.str.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class Os {
    public static final int WINDOWS = 3;
    public static final int _32Bit = -2;
    public static final int type;
    public static final int OSX_AMD64 = 1;
    public static final int LINUX_AMD64 = 2;
    public static final int LINUX_ARM64 = 4;
    public static final int FREEBSD = 5;
    public static final int OSX_ARM64 = 6;

    private Os() {
    }

    public static native long compareAndSwap(long mem, long oldValue, long newValue);

    public static native long currentTimeMicros();

    public static native long currentTimeNanos();

    public static native int errno();

    public static long forkExec(CharSequence args) {
        ObjList<Path> paths = Chars.splitLpsz(args);
        int n = paths.size();
        try {
            long argv = Unsafe.malloc((n + 1) * 8L, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = argv;
                for (int i = 0; i < n; i++) {
                    Unsafe.getUnsafe().putLong(p, paths.getQuick(i).address());
                    p += 8;
                }
                Unsafe.getUnsafe().putLong(p, 0);
                return forkExec(argv);
            } finally {
                Unsafe.free(argv, n + 1, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            for (int i = 0; i < n; i++) {
                paths.getQuick(i).close();
            }
        }
    }

    public static int forkExecPid(long forkExecT) {
        return Unsafe.getUnsafe().getInt(forkExecT + 8);
    }

    public static int forkExecReadFd(long forkExecT) {
        return Unsafe.getUnsafe().getInt(forkExecT);
    }

    public static int forkExecWriteFd(long forkExecT) {
        return Unsafe.getUnsafe().getInt(forkExecT + 4);
    }

    public static byte[] generateKerberosToken(CharSequence spn) throws KerberosException {
        try (CharSequenceZ cs = new CharSequenceZ(spn)) {
            final long struct = generateKrbToken(cs.address());
            int status = Unsafe.getUnsafe().getInt(struct);
            int bufLen = Unsafe.getUnsafe().getInt(struct + 4);
            long ptoken = Unsafe.getUnsafe().getLong(struct + 8);


            if (status != 0) {
                freeKrbToken(struct);
                throw new KerberosException(status);
            }

            byte[] token = new byte[bufLen];
            for (int i = 0; i < bufLen; i++) {
                token[i] = Unsafe.getUnsafe().getByte(ptoken + i);
            }
            freeKrbToken(struct);

            return token;
        }
    }

    public static native int getPid();

    @SuppressWarnings("EmptyMethod")
    public static void init() {
    }

    public static int setCurrentThreadAffinity(int cpu) {
        if (cpu == -1) {
            return 0;
        }
        return setCurrentThreadAffinity0(cpu);
    }

    public static void sleep(long millis) {
        final long t = System.currentTimeMillis();
        long deadline = millis;
        while (deadline > 0) {
            try {
                Thread.sleep(deadline);
                break;
            } catch (InterruptedException e) {
                deadline = System.currentTimeMillis() - t;
            }
        }
    }

    private static native int setCurrentThreadAffinity0(int cpu);

    private static native long generateKrbToken(long spn);

    private static native void freeKrbToken(long struct);

    private static native long forkExec(long argv);

    private static void loadLib(String lib) {
        InputStream is = Os.class.getResourceAsStream(lib);
        if (is == null) {
            throw new FatalError("Internal error: cannot find " + lib + ", broken package?");
        }

        try {
            File tempLib = null;
            try {
                int dot = lib.indexOf('.');
                tempLib = File.createTempFile(lib.substring(0, dot), lib.substring(dot));
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
                throw new FatalError("Internal error: cannot unpack " + tempLib, e);
            }
        } finally {
            Misc.free(is);
        }
    }

    static {
        if ("64".equals(System.getProperty("sun.arch.data.model"))) {
            String osName = System.getProperty("os.name");
            if (osName.contains("Linux")) {
                if ("aarch64".equals(System.getProperty("os.arch"))) {
                    type = LINUX_ARM64;
                    loadLib("/io/questdb/bin/armlinux/libquestdb.so");
                } else {
                    type = LINUX_AMD64;
                    loadLib("/io/questdb/bin/linux/libquestdb.so");
                }
            } else if (osName.contains("Mac")) {
                if ("aarch64".equals(System.getProperty("os.arch"))) {
                    type = OSX_ARM64;
                    loadLib("/io/questdb/bin/armosx/libquestdb.dylib");
                } else {
                    type = OSX_AMD64; // darwin
                    loadLib("/io/questdb/bin/osx/libquestdb.dylib");
                }
            } else if (osName.contains("Windows")) {
                type = WINDOWS;
                loadLib("/io/questdb/bin/windows/libquestdb.dll");
            } else if (osName.contains("FreeBSD")) {
                type = FREEBSD; // darwin is based on FreeBSD, so things that work for OSX will probably work for FreeBSD
                loadLib("/io/questdb/bin/freebsd/libquestdb.so");
            } else {
                throw new Error("Unsupported OS: " + osName);
            }
        } else {
            type = _32Bit;
        }
    }
}
