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

import io.questdb.std.ex.FatalError;
import io.questdb.std.ex.KerberosException;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

public final class Os {
    public static final int FREEBSD = 4;
    public static final int LINUX = 2;
    public static final int DARWIN = 1;
    public static final int ARCH_AARCH64 = 1;
    public static final int ARCH_AMD64 = 2;
    public static final long PARK_NANOS_MAX = 5 * 1_000_000_000L;
    public static final int WINDOWS = 3;
    public static final int _32Bit = -2;
    public static final int type;
    public static final String name;
    public static final int arch;
    public static final String archName;

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
                    Unsafe.getUnsafe().putLong(p, paths.getQuick(i).ptr());
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
        // We use Path as a LPSZ sink here.
        try (Path sink = new Path().of(spn).$()) {
            final long struct = generateKrbToken(sink.ptr());
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

    public static native int getEnvironmentType();

    public static native int getPid();

    /**
     * Returns physical memory used by this process (Resident Set Size/Working Set Size).
     *
     * @return used RSS memory in bytes
     */
    public static native long getRss();

    @SuppressWarnings("EmptyMethod")
    public static void init() {
    }

    public static boolean isLinux() {
        return type == LINUX;
    }

    public static boolean isOSX() {
        return type == DARWIN;
    }

    public static boolean isPosix() {
        return type != Os.WINDOWS;
    }

    public static native boolean isRustReleaseBuild();

    public static boolean isWindows() {
        return type == Os.WINDOWS;
    }

    public static void park() {
        LockSupport.parkNanos(Os.PARK_NANOS_MAX);
    }

    public static native long malloc(long size);

    public static native long realloc(long mem, long size);

    public static native void free(long mem);

    public static void pause() {
        try {
            Thread.sleep(0);
        } catch (InterruptedException ignore) {
        }
    }

    public static int setCurrentThreadAffinity(int cpu) {
        if (cpu == -1) {
            return 0;
        }
        return setCurrentThreadAffinity0(cpu);
    }

    public static void sleep(long millis) {
        long t = System.currentTimeMillis();
        long deadline = millis;
        while (deadline > 0) {
            try {
                Thread.sleep(deadline);
                break;
            } catch (InterruptedException e) {
                long t2 = System.currentTimeMillis();
                deadline -= t2 - t;
                t = t2;
            }
        }
    }

    public static native long smokeTest(long a, long b);

    private static native long forkExec(long argv);

    private static native void freeKrbToken(long struct);

    private static native long generateKrbToken(long spn);

    private static native void initRust();

    private static void loadLib(String lib) {
        InputStream is = Os.class.getResourceAsStream(lib);
        if (is == null) {
            throw new FatalError("Internal error: cannot find " + lib + ", broken package?");
        }
        loadLib(lib, is);
    }

    private static void loadLib(String lib, @NotNull InputStream libStream) {
        try {
            File tempLib = null;
            try {
                int dot = lib.indexOf('.');
                tempLib = File.createTempFile(lib.substring(0, dot), lib.substring(dot));
                // copy to tempLib
                try (FileOutputStream out = new FileOutputStream(tempLib)) {
                    byte[] buf = new byte[4096];
                    while (true) {
                        int read = libStream.read(buf);
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
            Misc.free(libStream);
        }
    }

    private static native int setCurrentThreadAffinity0(int cpu);

    static {
        if ("aarch64".equals(System.getProperty("os.arch"))) {
            arch = ARCH_AARCH64;
            archName = "aarch64";
        } else {
            arch = ARCH_AMD64;
            archName = "amd64";
        }

        if ("64".equals(System.getProperty("sun.arch.data.model"))) {
            String osName = System.getProperty("os.name");
            String outputLibExt;

            if (osName.contains("Linux")) {
                name = "linux";
                outputLibExt = ".so";
                type = LINUX;
            } else if (osName.contains("Mac")) {
                name = "darwin";
                outputLibExt = ".dylib";
                type = DARWIN;
            } else if (osName.contains("Windows")) {
                name = "windows";
                outputLibExt = ".dll";
                type = WINDOWS;
            } else if (osName.contains("FreeBSD")) {
                name = "freebsd";
                outputLibExt = ".so";
                type = FREEBSD; // darwin is based on FreeBSD, so things that work for OSX will probably work for FreeBSD
            } else {
                throw new Error("Unsupported OS: " + osName);
            }

            String prdLibRoot = "/io/questdb/bin/" + name + '-' + archName + '/';
            String devCXXLibRoot = "/io/questdb/bin-local/";
            String cxxLibName = "libquestdb" + outputLibExt;
            String devCXXLib = devCXXLibRoot + cxxLibName;

            // try dev CXX lib first
            InputStream libCXXStream = Os.class.getResourceAsStream(devCXXLib);
            if (libCXXStream == null) {
                loadLib(prdLibRoot + cxxLibName);
            } else {
                System.err.println("Loading DEV CXX library: " + devCXXLib);
                loadLib(devCXXLib, libCXXStream);
            }

            // Rust library is loaded conditionally to allow for convenience
            // of the development environments that target Rust source code
            // The library file is missing "lib" prefix on Windows
            String devRustLibRoot = "/io/questdb/rust/";
            final String rustLibName;
            if (type == WINDOWS) {
                rustLibName = "questdbr" + outputLibExt;
            } else {
                rustLibName = "libquestdbr" + outputLibExt;
            }

            final String devRustLib = devRustLibRoot + rustLibName;

            InputStream libRustStream = Os.class.getResourceAsStream(devRustLib);
            if (libRustStream == null) {
                loadLib(prdLibRoot + rustLibName);
            } else {
                loadLib(devRustLib, libRustStream);
            }
            initRust();
        } else {
            type = _32Bit;
            name = System.getProperty("os.name");
        }
    }
}
