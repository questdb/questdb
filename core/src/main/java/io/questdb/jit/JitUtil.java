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

package io.questdb.jit;

import io.questdb.std.Os;

import java.util.function.IntSupplier;

public final class JitUtil {
    /**
     * The backend compiles {@link CompiledFilterIRSerializer#SYM_IN_SET}. Mirrors
     * {@code jit_features::kFeatureSymInSet} in {@code jit/common.h}.
     */
    public static final int FEATURE_SYM_IN_SET = 1;

    private JitUtil() {
    }

    public static boolean isJitSupported() {
        return Os.arch == Os.ARCH_X86_64 || Os.arch == Os.ARCH_AARCH64;
    }

    /**
     * Whether the loaded native library compiles a symbol IN list's membership test
     * ({@link CompiledFilterIRSerializer#SYM_IN_SET}). Only the x86-64 backends implement it, and
     * only a library built since the opcode exists says so: a library without the
     * {@code getFeatures} function - any custom {@code questdb.libs.dir}, or an older build paired
     * with this jar - reads as no capability, so the serializer never sends it the opcode.
     */
    public static boolean isSymbolInSetSupported() {
        return Os.arch == Os.ARCH_X86_64 && (NativeFeatures.FEATURES & FEATURE_SYM_IN_SET) != 0;
    }

    /**
     * Reads a native feature mask, treating a library that does not export the function as having
     * no features.
     */
    public static int readFeatures(IntSupplier nativeFeatures) {
        try {
            return nativeFeatures.getAsInt();
        } catch (UnsatisfiedLinkError e) {
            return 0;
        }
    }

    // Probed once, on first use, after Os has loaded the native library.
    private static class NativeFeatures {
        private static final int FEATURES;

        static {
            Os.init();
            FEATURES = isJitSupported() ? readFeatures(FiltersCompiler::getFeatures) : 0;
        }
    }
}
