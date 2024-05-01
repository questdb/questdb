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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.StaticUsernamePasswordMatcher;
import io.questdb.cutlass.pgwire.UsernamePasswordMatcher;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class StaticUsernamePasswordMatcherTest {

    @Test
    public void testVerifyPassword() {
        StaticUsernamePasswordMatcher matcher = newMatcher();
        assertMatch(matcher);
        assertNoMatch(matcher, "user", "ssap");
        assertNoMatch(matcher, "user", "wrongpassword");
        assertNoMatch(matcher, "", "wrongpassword");
        assertNoMatch(matcher, null, "pass");
    }

    private static void assertMatch(UsernamePasswordMatcher matcher) {
        byte[] bytes = "pass".getBytes(StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }
            assertMatch(matcher, ptr, bytes.length);
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertMatch(UsernamePasswordMatcher matcher, long passwordPtr, int passwordLen) {
        Assert.assertTrue(matcher.verifyPassword("user", passwordPtr, passwordLen));
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

    private StaticUsernamePasswordMatcher newMatcher() {
        return new StaticUsernamePasswordMatcher("user", "pass");
    }
}
