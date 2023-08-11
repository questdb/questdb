package io.questdb.cutlass.tls;

public class NativeTls {
    private static final int PLAIN_TEXT_AVAILABLE = 1 << 2;
    private static final int READ_FROM_SOCKET = 1 << 3;
    private static final int WANTS_READ = 1 << 1;
    private static final int WANTS_WRITE = 1 << 0;
    private static final int WROTE_TO_SOCKET = 1 << 4;

    public static native void freeSession(long sessionPtr);

    public static native long fromFd(int fd);

    public static int getFlagsInt(long result) {
        // highest 32 bits
        return (int) ((result >>> 32) & 0xFFFFFFFF);
    }

    public static int getIntResult(long result) {
        // lowest 32 bits
        return (int) (result & 0xFFFFFFFF);
    }

    public static native boolean isHandshakeComplete(long sessionPtr);

    public static native long newSession();

    public static boolean plainTextAvailable(int flags) {
        return (flags & PLAIN_TEXT_AVAILABLE) != 0;
    }

    public static boolean readFromSocket(int flags) {
        return (flags & READ_FROM_SOCKET) != 0;
    }

    public static native long readTls(int fd, long sessionPtr, long bufferPtr, int bufferLen);

    public static native void test();

    public static boolean wantsRead(int flags) {
        return (flags & WANTS_READ) != 0;
    }

    public static boolean wantsWrite(int flags) {
        return (flags & WANTS_WRITE) != 0;
    }

    public static native long writeTls(int fd, long sessionPtr, long bufferPtr, int bufferLen);

    public static boolean wroteToSocket(int flags) {
        return (flags & WROTE_TO_SOCKET) != 0;
    }


}
