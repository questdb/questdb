package io.questdb.cutlass.pgwire;

import io.questdb.std.Unsafe;

public class PGKeywords {
    public static boolean isOptions(long lpsz, long len) {
        if (len != 7) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'p'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'i'
                && Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'n'
                && Unsafe.getUnsafe().getByte(i) == 's';
    }

    public static boolean isUser(long lpsz, long len) {
        if (len != 4) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == 'u'
                && Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i) == 'r';
    }

    //"-c statement_timeout="
    public static boolean startsWithTimeoutOption(long lpsz, long len) {
        if (len < 21) {
            return false;
        }
        long i = lpsz;
        return Unsafe.getUnsafe().getByte(i++) == '-'
                && Unsafe.getUnsafe().getByte(i++) == 'c'
                && Unsafe.getUnsafe().getByte(i++) == ' '
                && Unsafe.getUnsafe().getByte(i++) == 's'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'a'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'm'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'n'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == '_'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i++) == 'i'
                && Unsafe.getUnsafe().getByte(i++) == 'm'
                && Unsafe.getUnsafe().getByte(i++) == 'e'
                && Unsafe.getUnsafe().getByte(i++) == 'o'
                && Unsafe.getUnsafe().getByte(i++) == 'u'
                && Unsafe.getUnsafe().getByte(i++) == 't'
                && Unsafe.getUnsafe().getByte(i) == '=';
    }
}
