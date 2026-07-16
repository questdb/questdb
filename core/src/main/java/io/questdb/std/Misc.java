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

package io.questdb.std;

import io.questdb.std.ex.FatalError;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

public final class Misc {
    public static final int CACHE_LINE_SIZE = 64;
    public static final String EOL = "\r\n";
    private static final CarrierLocal<Decimal128> tlDecimal128 = new CarrierLocal<>(Decimal128::new);
    private static final CarrierLocal<Decimal256> tlDecimal256 = new CarrierLocal<>(Decimal256::new);
    private static final CarrierLocal<StringSink> tlSink = new CarrierLocal<>(StringSink::new);
    private static final CarrierLocal<Utf8StringSink> tlUtf8Sink = new CarrierLocal<>(Utf8StringSink::new);
    private static final CarrierLocal<Utf8SinkPool> tlUtf8SinkPool = new CarrierLocal<>(Utf8SinkPool::new);

    private Misc() {
    }

    /**
     * Acquire a pooled Utf8StringSink for use in recursive/nested contexts.
     * <p>
     * IMPORTANT: Every call to acquireUtf8Sink() MUST be paired with a call to
     * releaseUtf8Sink() in a finally block.
     * <p>
     * Example usage:
     * <pre>
     * Utf8StringSink sink = Misc.acquireUtf8Sink();
     * try {
     *     sink.put(data);
     *     // ... use sink, including recursive calls that also acquire
     * } finally {
     *     Misc.releaseUtf8Sink();
     * }
     * </pre>
     *
     * @return a cleared Utf8StringSink from the pool
     */
    public static Utf8StringSink acquireUtf8Sink() {
        return tlUtf8SinkPool.get().acquire();
    }

    public static <T extends Mutable> T clear(T object) {
        if (object != null) {
            object.clear();
        }
        return null;
    }

    public static <T extends Mutable> void clear(T[] list) {
        if (list != null) {
            for (T t : list) {
                Misc.clear(t);
            }
        }
    }

    public static void clearObjList(ObjList<? extends Mutable> args) {
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                Mutable m = args.getQuick(i);
                if (m != null) {
                    m.clear();
                }
            }
        }
    }

    public static <T extends Closeable> T free(T object) {
        if (object != null) {
            try {
                object.close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    /**
     * Closes the object and adds any close failure to {@code primary} as a suppressed
     * exception. This method lets callers continue a multi-resource cleanup without
     * masking the primary failure.
     */
    public static <T extends Closeable> void free(@Nullable T object, @NotNull Throwable primary) {
        try {
            free(object);
        } catch (Throwable th) {
            if (th != primary) {
                primary.addSuppressed(th);
            }
        }
    }

    public static <T extends Closeable> void free(T[] list) {
        if (list != null) {
            for (int i = 0, n = list.length; i < n; i++) {
                list[i] = Misc.free(list[i]);
            }
        }
    }

    /**
     * Closes the object while keeping multi-resource cleanup best-effort: a close() failure
     * folds into the given failure chain instead of propagating, so later resources in the
     * same cleanup sequence still see a close attempt. The first failure becomes the primary
     * and later failures attach to it as suppressed. Callers thread the returned chain
     * through the whole sequence and rethrow it at the appropriate package boundary. Callers
     * that already have a non-null primary failure should use
     * {@link #free(Closeable, Throwable)} instead.
     */
    public static <T extends Closeable> @Nullable Throwable freeBestEffort(@Nullable Throwable primary, @Nullable T object) {
        if (primary != null) {
            free(object, primary);
            return primary;
        }
        try {
            free(object);
            return null;
        } catch (Throwable th) {
            return th;
        }
    }

    // same as free() but can be used when input object type is not guaranteed to be Closeable
    public static <T> T freeIfCloseable(T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (IOException e) {
                throw new FatalError(e);
            }
        }
        return null;
    }

    /**
     * Closes an optionally-closeable object while keeping multi-resource cleanup best-effort.
     */
    public static <T> @Nullable Throwable freeIfCloseableBestEffort(@Nullable Throwable primary, @Nullable T object) {
        if (object instanceof Closeable closeable) {
            return freeBestEffort(primary, closeable);
        }
        return primary;
    }

    public static <T extends Closeable> void freeObjList(ObjList<T> list) {
        if (list != null) {
            freeObjList0(list);
        }
    }

    /**
     * Closes every list entry and adds close failures to {@code primary} as suppressed
     * exceptions. The method nulls each slot before its close attempt and continues after
     * failures. Do not pass list subclasses that reject {@code setQuick()}.
     */
    public static <T extends Closeable> void freeObjList(@Nullable ObjList<T> list, @NotNull Throwable primary) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                final T object = list.getQuick(i);
                list.setQuick(i, null);
                free(object, primary);
            }
        }
    }

    public static <T extends Closeable> void freeObjListAndClear(ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                free(list.getQuick(i));
            }
            list.clear();
        }
    }

    public static <T extends Closeable> void freeObjListAndKeepObjects(ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                free(list.getQuick(i));
            }
        }
    }

    /**
     * Closes every list entry while retaining the entries and keeping cleanup best-effort.
     */
    public static <T extends Closeable> @Nullable Throwable freeObjListAndKeepObjectsBestEffort(
            @Nullable Throwable primary,
            @Nullable ObjList<T> list
    ) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                primary = freeBestEffort(primary, list.getQuick(i));
            }
        }
        return primary;
    }

    /**
     * Closes every list entry and nulls its slot even when earlier entries throw, folding
     * close() failures into the given failure chain the same way
     * {@link #freeBestEffort(Throwable, Closeable)} does. Callers that already have a non-null
     * primary failure should use {@link #freeObjList(ObjList, Throwable)} instead. Do not pass
     * list subclasses that reject {@code setQuick()}.
     */
    public static <T extends Closeable> @Nullable Throwable freeObjListBestEffort(@Nullable Throwable primary, @Nullable ObjList<T> list) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                final T object = list.getQuick(i);
                list.setQuick(i, null);
                primary = freeBestEffort(primary, object);
            }
        }
        return primary;
    }

    // same as freeObjList() but can be used when input object type is not guaranteed to be Closeable
    public static <T> void freeObjListIfCloseable(ObjList<T> list) {
        if (list != null) {
            freeObjList0(list);
        }
    }

    /**
     * Closes every optionally-closeable list entry and nulls its slot while keeping cleanup best-effort.
     */
    public static <T> @Nullable Throwable freeObjListIfCloseableBestEffort(
            @Nullable Throwable primary,
            @Nullable ObjList<T> list
    ) {
        if (list != null) {
            for (int i = 0, n = list.size(); i < n; i++) {
                final T object = list.getQuick(i);
                list.setQuick(i, null);
                primary = freeIfCloseableBestEffort(primary, object);
            }
        }
        return primary;
    }

    // same as freeObjListIfCloseable() but for arrays
    public static <T> void freeIfCloseable(T[] array) {
        if (array != null) {
            for (int i = 0, n = array.length; i < n; i++) {
                array[i] = freeIfCloseable(array[i]);
            }
        }
    }

    public static Decimal128 getThreadLocalDecimal128() {
        return tlDecimal128.get();
    }

    public static Decimal256 getThreadLocalDecimal256() {
        return tlDecimal256.get();
    }

    public static StringSink getThreadLocalSink() {
        StringSink b = tlSink.get();
        b.clear();
        return b;
    }

    public static Utf8StringSink getThreadLocalUtf8Sink() {
        Utf8StringSink b = tlUtf8Sink.get();
        b.clear();
        return b;
    }

    public static int[] getWorkerAffinity(int workerCount) {
        int[] res = new int[workerCount];
        Arrays.fill(res, -1);
        return res;
    }

    public static void releaseUtf8Sink() {
        tlUtf8SinkPool.get().release();
    }

    private static <T> void freeObjList0(ObjList<T> list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            list.setQuick(i, freeIfCloseable(list.getQuick(i)));
        }
    }

    private static class Utf8SinkPool {
        private final ObjList<Utf8StringSink> pool = new ObjList<>();
        private int depth;

        Utf8StringSink acquire() {
            if (depth >= pool.size()) {
                pool.add(new Utf8StringSink());
            }
            Utf8StringSink sink = pool.getQuick(depth++);
            sink.clear();
            return sink;
        }

        void release() {
            assert depth > 0;
            depth--;
        }
    }
}
