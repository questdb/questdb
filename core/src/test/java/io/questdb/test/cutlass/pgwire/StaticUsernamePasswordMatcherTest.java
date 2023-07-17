package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.StaticUsernamePasswordMatcher;
import io.questdb.cutlass.pgwire.UsernamePasswordMatcher;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class StaticUsernamePasswordMatcherTest {

    public static void assertMatch(UsernamePasswordMatcher matcher, String username, String password) {
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

    public static void assertMatch(UsernamePasswordMatcher matcher, String username, long passwordPtr, int passwordLen) {
        Assert.assertTrue(matcher.verifyPassword(username, passwordPtr, passwordLen));
    }

    public static void assertNoMatch(UsernamePasswordMatcher matcher, String username, long passwordPtr, int passwordLen) {
        Assert.assertFalse(matcher.verifyPassword(username, passwordPtr, passwordLen));
    }

    public static void assertNoMatch(UsernamePasswordMatcher matcher, String username, String password) {
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

    @Test
    public void testAfterClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, "roUser", "roPass");
            StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg);
            matcher.close();
            assertNoMatch(matcher, "user", "pass");
            assertNoMatch(matcher, "roUser", "roPass");
        });
    }

    @Test
    public void testEmptyDefaultPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "", true, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertNoMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testEmptyDefaultUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("", "pass", true, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertNoMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testEmptyRoPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, "roUser", "");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testEmptyRoUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, "", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testNullDefaultPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "", true, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertNoMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testNullDefaultUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration(null, "pass", true, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertNoMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testNullRoPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, "roUser", null);
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testNullRoUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, null, "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testRoDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", false, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, null, "pass");
                assertNoMatch(matcher, "user", "ssap");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    @Test
    public void testRoEnabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PGWireConfiguration cfg = new MyPGWireConfiguration("user", "pass", true, "roUser", "roPass");
            try (StaticUsernamePasswordMatcher matcher = new StaticUsernamePasswordMatcher(cfg)) {
                assertMatch(matcher, "user", "pass");
                assertNoMatch(matcher, "resu", "pass");
                assertNoMatch(matcher, "user", "bad-password");
                assertNoMatch(matcher, "user", "ssap");
                assertMatch(matcher, "roUser", "roPass");
                assertNoMatch(matcher, "resUor", "roPass");
                assertNoMatch(matcher, "roUser", "bad-password");
                assertNoMatch(matcher, "user", 42, 0);
                assertNoMatch(matcher, "roUser", 42, 0);
                assertNoMatch(matcher, "user", 0, 0);
                assertNoMatch(matcher, "roUser", 0, 0);
            }
        });
    }

    private static class MyPGWireConfiguration extends DefaultPGWireConfiguration {
        private final String defaultPassword;
        private final String defaultUsername;
        private final String readOnlyPassword;
        private final boolean readOnlyUserEnabled;
        private final String readOnlyUsername;


        private MyPGWireConfiguration(String defaultUsername, String defaultPassword, boolean readOnlyUserEnabled, String readOnlyUsername, String readOnlyPassword) {
            this.defaultUsername = defaultUsername;
            this.defaultPassword = defaultPassword;
            this.readOnlyUserEnabled = readOnlyUserEnabled;
            this.readOnlyUsername = readOnlyUsername;
            this.readOnlyPassword = readOnlyPassword;
        }

        @Override
        public String getDefaultPassword() {
            return defaultPassword;
        }

        @Override
        public String getDefaultUsername() {
            return defaultUsername;
        }

        @Override
        public String getReadOnlyPassword() {
            return readOnlyPassword;
        }

        @Override
        public String getReadOnlyUsername() {
            return readOnlyUsername;
        }

        @Override
        public boolean isReadOnlyUserEnabled() {
            return readOnlyUserEnabled;
        }
    }
}
