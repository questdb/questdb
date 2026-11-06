/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

/**
 * Experimental JDK 25 Foreign Function &amp; Memory (Panama) binding to the native
 * regex backend exported by {@code libquestdbr} (see {@code core/rust/qdbr/src/regex_match}).
 * <p>
 * This is a prototype gated behind {@link io.questdb.cairo.CairoConfiguration#isVarcharRegexNativeEnabled()};
 * for now it backs only the {@code ~} operator over VARCHAR. The hot {@code find} downcall is
 * linked with {@link Linker.Option#critical(boolean)} so it skips the Java&rarr;native thread-state
 * transition: matching a short native UTF-8 slice costs close to a regular JIT-compiled call.
 * <p>
 * If linking fails for any reason (older native library without the symbols, FFM lookup
 * failure, ...) {@link #AVAILABLE} stays {@code false} and callers transparently fall back
 * to the JDK {@code java.util.regex} engine.
 */
final class NativeRegex {
    static final boolean AVAILABLE;
    private static final Log LOG = LogFactory.getLog(NativeRegex.class);
    private static final MethodHandle MH_COMPILE;
    private static final MethodHandle MH_FIND;
    private static final MethodHandle MH_FREE;

    private NativeRegex() {
    }

    /**
     * Compiles a regex pattern using the native engine.
     *
     * @return a non-zero opaque handle on success, or {@code 0} if the engine is unavailable or
     * the pattern is unsupported (e.g. backreferences/lookaround) or invalid. A {@code 0} result
     * is the signal for the caller to fall back to the JDK engine.
     */
    static long compile(CharSequence pattern) {
        if (!AVAILABLE || pattern == null) {
            return 0;
        }
        final byte[] utf8 = pattern.toString().getBytes(StandardCharsets.UTF_8);
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment seg = arena.allocate(Math.max(1, utf8.length));
            MemorySegment.copy(utf8, 0, seg, ValueLayout.JAVA_BYTE, 0, utf8.length);
            return (long) MH_COMPILE.invokeExact(seg.address(), (long) utf8.length);
        } catch (Throwable t) {
            return 0;
        }
    }

    /**
     * Unanchored match, equivalent to {@code Matcher#find()}. {@code textPtr} must address
     * {@code textLen} contiguous native UTF-8 bytes (or be {@code 0} when {@code textLen == 0}).
     */
    static boolean find(long handle, long textPtr, int textLen) {
        try {
            return (int) MH_FIND.invokeExact(handle, textPtr, (long) textLen) != 0;
        } catch (Throwable t) {
            throw new RuntimeException("native regex find failed", t);
        }
    }

    static void free(long handle) {
        if (handle == 0) {
            return;
        }
        try {
            MH_FREE.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException("native regex free failed", t);
        }
    }

    static {
        boolean available = false;
        MethodHandle compile = null;
        MethodHandle find = null;
        MethodHandle free = null;
        try {
            Os.init(); // make sure libquestdbr has been System.load()-ed before loaderLookup()
            final Linker linker = Linker.nativeLinker();
            final SymbolLookup lookup = SymbolLookup.loaderLookup();
            compile = linker.downcallHandle(
                    lookup.find("qdb_regex_compile").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
            );
            find = linker.downcallHandle(
                    lookup.find("qdb_regex_find").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG),
                    // hot path: no JVM callbacks, no heap access, returns promptly
                    Linker.Option.critical(false)
            );
            free = linker.downcallHandle(
                    lookup.find("qdb_regex_free").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
            );
            available = true;
            LOG.info().$("native regex backend enabled (FFM)").$();
        } catch (Throwable t) {
            LOG.info().$("native regex backend unavailable, using JDK java.util.regex [reason=").$(t.getMessage()).$(']').$();
        }
        MH_COMPILE = compile;
        MH_FIND = find;
        MH_FREE = free;
        AVAILABLE = available;
    }
}
