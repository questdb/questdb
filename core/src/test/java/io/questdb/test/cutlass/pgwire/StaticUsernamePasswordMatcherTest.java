package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.StaticUsernamePasswordMatcher;
import io.questdb.cutlass.pgwire.UsernamePasswordMatcher;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class StaticUsernamePasswordMatcherTest {
    private int passwordLen;
    private long passwordPtr;

    @After
    public void tearDown() {
        if (passwordPtr != 0) {
            Unsafe.free(passwordPtr, passwordLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVerifyPassword() {
        StaticUsernamePasswordMatcher matcher = newMatcher("user", "pass");
        assertMatch(matcher, "user", "pass");
        assertNoMatch(matcher, "user", "ssap");
        assertNoMatch(matcher, "user", "wrongpassword");
        assertNoMatch(matcher, "", "wrongpassword");
        assertNoMatch(matcher, null, "pass");
    }

    private static void assertMatch(UsernamePasswordMatcher matcher, String username, String password) {
        byte[] bytes = password.getBytes(StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            assertMatch(matcher, username, ptr, bytes.length);
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertMatch(UsernamePasswordMatcher matcher, String username, long passwordPtr, int passwordLen) {
        Assert.assertTrue(matcher.verifyPassword(username, passwordPtr, passwordLen));
    }

    private static void assertNoMatch(UsernamePasswordMatcher matcher, String username, long passwordPtr, int passwordLen) {
        Assert.assertFalse(matcher.verifyPassword(username, passwordPtr, passwordLen));
    }

    private static void assertNoMatch(UsernamePasswordMatcher matcher, String username, String password) {
        byte[] bytes = password.getBytes(StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            assertNoMatch(matcher, username, ptr, bytes.length);
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private StaticUsernamePasswordMatcher newMatcher(String username, String password) {
        assert passwordPtr == 0;
        assert passwordLen == 0;

        byte[] bytes = password.getBytes(StandardCharsets.UTF_8);
        passwordLen = bytes.length;
        passwordPtr = Unsafe.malloc(passwordLen, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(passwordPtr + i, bytes[i]);
        }
        return new StaticUsernamePasswordMatcher(username, passwordPtr, passwordLen);
    }
}
