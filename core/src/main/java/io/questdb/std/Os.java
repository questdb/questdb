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

import io.questdb.std.ex.FatalError;
import io.questdb.std.ex.KerberosException;
import io.questdb.std.str.CharSequenceZ;
import io.questdb.std.str.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

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

    public static native int translateSysErrno(int errno);

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

    public static void pause() {
        if (Os.type != Os.WINDOWS) {
            LockSupport.parkNanos(1);
        } else {
            try {
                Thread.sleep(0);
            } catch (InterruptedException ignore) {
            }
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

    public static class Errno {
        public static final int EPERM = 1;
        public static final int ENOENT = 2;
        public static final int ESRCH = 3;
        public static final int EINTR = 4;
        public static final int EIO = 5;
        public static final int ENXIO = 6;
        public static final int E2BIG = 7;
        public static final int ENOEXEC = 8;
        public static final int EBADF = 9;
        public static final int ECHID = 10;
        public static final int ENOMEM = 12;
        public static final int EACCES = 13;
        public static final int EFAUT = 14;
        public static final int EBUSY = 16;
        public static final int EEXIST = 17;
        public static final int EXDEV = 18;
        public static final int ENODEV = 19;
        public static final int ENOTDIR = 20;
        public static final int EISDIR = 21;
        public static final int EINVA = 22;
        public static final int ENFIE = 23;
        public static final int EMFIE = 24;
        public static final int ENOTTY = 25;
        public static final int EFBIG = 27;
        public static final int ENOSPC = 28;
        public static final int ESPIPE = 29;
        public static final int EROFS = 30;
        public static final int EMINK = 31;
        public static final int EPIPE = 32;
        public static final int EDOM = 33;
        public static final int ERANGE = 34;
    }

    public static class LinuxErrno extends Errno {
        public static final int ENOTBK = 15;
        public static final int ETXTBSY = 26;
        public static final int EDEADK = 35;
        public static final int EWOULDBLOCK = 11;
        public static final int EAGAIN = 11;
        public static final int EINPROGRESS = 115;
        public static final int EALREADY = 114;
        public static final int ENOTSOCK = 88;
        public static final int EDESTADDRREQ = 89;
        public static final int EMSGSIZE = 90;
        public static final int EPROTOTYPE = 91;
        public static final int ENOPROTOOPT = 92;
        public static final int EPROTONOSUPPORT = 93;
        public static final int ESOCKTNOSUPPORT = 94;
        public static final int EOPNOTSUPP = 95;
        public static final int EPFNOSUPPORT = 96;
        public static final int EAFNOSUPPORT = 97;
        public static final int EADDRINUSE = 98;
        public static final int EADDRNOTAVAIL = 99;
        public static final int ENETDOWN = 100;
        public static final int ENETUNREACH = 101;
        public static final int ENETRESET = 102;
        public static final int ECONNABORTED = 103;
        public static final int ECONNRESET = 104;
        public static final int ENOBUFS = 105;
        public static final int EISCONN = 106;
        public static final int ENOTCONN = 107;
        public static final int ESHUTDOWN = 108;
        public static final int ETOOMANYREFS = 109;
        public static final int ETIMEDOUT = 110;
        public static final int ECONNREFUSED = 111;
        public static final int ELOOP = 40;
        public static final int ENAMETOOLONG = 36;
        public static final int EHOSTDOWN = 112;
        public static final int EHOSTUNREACH = 113;
        public static final int ENOTEMPTY = 39;
        public static final int EUSERS = 87;
        public static final int EDQUOT = 122;
        public static final int ESTALE = 116;
        public static final int EREMOTE = 66;
        public static final int ENOLCK = 37;
        public static final int ENOSYS = 38;
        public static final int EOVERFLOW = 75;
        public static final int EIDRM = 43;
        public static final int ENOMSG = 42;
        public static final int EILSEQ = 84;
        public static final int EBADMSG = 74;
        public static final int EMULTIHOP = 72;
        public static final int ENODATA = 61;
        public static final int ENOLINK = 67;
        public static final int ENOSR = 63;
        public static final int ENOSTR = 60;
        public static final int EPROTO = 71;
        public static final int ETIME = 62;
        public static final int ECHRNG = 44;
        public static final int EL2NSYNC = 45;
        public static final int EL3HLT = 46;
        public static final int EL3RST = 47;
        public static final int ELNRNG = 48;
        public static final int EUNATCH = 49;
        public static final int ENOCSI = 50;
        public static final int EL2HLT = 51;
        public static final int EBADE = 52;
        public static final int EBADR = 53;
        public static final int EXFULL = 54;
        public static final int ENOANO = 55;
        public static final int EBADRQC = 56;
        public static final int EBADSLT = 57;
        public static final int EDEADLOCK = 35;
        public static final int EBFONT = 59;
        public static final int ENONET = 64;
        public static final int ENOPKG = 65;
        public static final int EADV = 68;
        public static final int ESRMNT = 69;
        public static final int ECOMM = 70;
        public static final int EDOTDOT = 73;
        public static final int ENOTUNIQ = 76;
        public static final int EBADFD = 77;
        public static final int EREMCHG = 78;
        public static final int ELIBACC = 79;
        public static final int ELIBBAD = 80;
        public static final int ELIBSCN = 81;
        public static final int ELIBMAX = 82;
        public static final int ELIBEXEC = 83;
        public static final int ERESTART = 85;
        public static final int ESTRPIPE = 86;
        public static final int EUCLEAN = 117;
        public static final int ENOTNAM = 118;
        public static final int ENAVAIL = 119;
        public static final int EISNAM = 120;
        public static final int EREMOTEIO = 121;
        public static final int ECANCELED = 125;
        public static final int EKEYEXPIRED = 127;
        public static final int EKEYREJECTED = 129;
        public static final int EKEYREVOKED = 128;
        public static final int EMEDIUMTYPE = 124;
        public static final int ENOKEY = 126;
        public static final int ENOMEDIUM = 123;
        public static final int ENOTRECOVERABLE = 131;
        public static final int EOWNERDEAD = 130;
        public static final int ERFKILL = 132;
        public static final int ENOTSUP = 95;
        public static final int EHWPOISON = 133;
    }

    public static class OsxErrno extends Errno {
        public static final int ENOTBK = 15;
        public static final int ETXTBSY = 26;
        public static final int EDEADLK = 11;
        public static final int EWOULDBLOCK = 35;
        public static final int EAGAIN = 35;
        public static final int EINPROGRESS = 36;
        public static final int EALREADY = 37;
        public static final int ENOTSOCK = 38;
        public static final int EDESTADDRREQ = 39;
        public static final int EMSGSIZE = 40;
        public static final int EPROTOTYPE = 41;
        public static final int ENOPROTOOPT = 42;
        public static final int EPROTONOSUPPORT = 43;
        public static final int ESOCKTNOSUPPORT = 44;
        public static final int EOPNOTSUPP = 102;
        public static final int EPFNOSUPPORT = 46;
        public static final int EAFNOSUPPORT = 47;
        public static final int EADDRINUSE = 48;
        public static final int EADDRNOTAVAIL = 49;
        public static final int ENETDOWN = 50;
        public static final int ENETUNREACH = 51;
        public static final int ENETRESET = 52;
        public static final int ECONNABORTED = 53;
        public static final int ECONNRESET = 54;
        public static final int ENOBUFS = 55;
        public static final int EISCONN = 56;
        public static final int ENOTCONN = 57;
        public static final int ESHUTDOWN = 58;
        public static final int ETOOMANYREFS = 59;
        public static final int ETIMEDOUT = 60;
        public static final int ECONNREFUSED = 61;
        public static final int ELOOP = 62;
        public static final int ENAMETOOLONG = 63;
        public static final int EHOSTDOWN = 64;
        public static final int EHOSTUNREACH = 65;
        public static final int ENOTEMPTY = 66;
        public static final int EUSERS = 68;
        public static final int EDQUOT = 69;
        public static final int ESTALE = 70;
        public static final int EREMOTE = 71;
        public static final int ENOLCK = 77;
        public static final int ENOSYS = 78;
        public static final int EOVERFLOW = 84;
        public static final int EIDRM = 90;
        public static final int ENOMSG = 91;
        public static final int EILSEQ = 92;
        public static final int EBADMSG = 94;
        public static final int EMULTIHOP = 95;
        public static final int ENODATA = 96;
        public static final int ENOLINK = 97;
        public static final int ENOSR = 98;
        public static final int ENOSTR = 99;
        public static final int EPROTO = 100;
        public static final int ETIME = 101;
        public static final int ECANCELED = 89;
        public static final int ENOTRECOVERABLE = 104;
        public static final int EOWNERDEAD = 105;
        public static final int EAUTH = 80;
        public static final int EBADRPC = 72;
        public static final int EFTYPE = 79;
        public static final int ENEEDAUTH = 81;
        public static final int ENOATTR = 93;
        public static final int ENOTSUP = 45;
        public static final int EPROCLIM = 67;
        public static final int EPROCUNAVAIL = 76;
        public static final int EPROGMISMATCH = 75;
        public static final int EPROGUNAVAIL = 74;
        public static final int ERPCMISMATCH = 73;
    }

    public static class WinErrno extends Errno {
        public static final int ETXTBSY = 139;
        public static final int EWOULDBLOCK = 140;
        public static final int EAGAIN = 11;
        public static final int EINPROGRESS = 112;
        public static final int EALREADY = 103;
        public static final int ENOTSOCK = 128;
        public static final int EDESTADDRREQ = 109;
        public static final int EMSGSIZE = 115;
        public static final int EPROTOTYPE = 136;
        public static final int ENOPROTOOPT = 123;
        public static final int EPROTONOSUPPORT = 135;
        public static final int EOPNOTSUPP = 130;
        public static final int EAFNOSUPPORT = 102;
        public static final int EADDRINUSE = 100;
        public static final int EADDRNOTAVAIL = 101;
        public static final int ENETDOWN = 116;
        public static final int ENETUNREACH = 118;
        public static final int ENETRESET = 117;
        public static final int ECONNABORTED = 106;
        public static final int ECONNRESET = 108;
        public static final int ENOBUFS = 119;
        public static final int EISCONN = 113;
        public static final int ENOTCONN = 126;
        public static final int ETIMEDOUT = 138;
        public static final int ECONNREFUSED = 107;
        public static final int ELOOP = 114;
        public static final int ENAMETOOLONG = 38;
        public static final int EHOSTUNREACH = 110;
        public static final int ENOTEMPTY = 41;
        public static final int ENOLCK = 39;
        public static final int ENOSYS = 40;
        public static final int EOVERFLOW = 132;
        public static final int EIDRM = 111;
        public static final int ENOMSG = 122;
        public static final int EILSEQ = 42;
        public static final int EBADMSG = 104;
        public static final int ENODATA = 120;
        public static final int ENOLINK = 121;
        public static final int ENOSR = 124;
        public static final int ENOSTR = 125;
        public static final int EPROTO = 134;
        public static final int ETIME = 137;
        public static final int EDEADLOCK = 36;
        public static final int ECANCELED = 105;
        public static final int ENOTRECOVERABLE = 127;
        public static final int EOWNERDEAD = 133;
        public static final int ENOTSUP = 129;
    }
}
