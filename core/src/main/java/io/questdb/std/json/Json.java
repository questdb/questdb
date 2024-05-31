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

package io.questdb.std.json;

import io.questdb.std.Os;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;

public class Json {
    public static final int SIMDJSON_PADDING;

    private static native int validate(long s, long len, long capacity);

    private static native void queryPath(
            long jsonPtr,
            long jsonLen,
            long jsonCapacity,
            long pathPtr,
            long pathLen,
            long resultPtr,
            long destPtr,
            int maxSize
    );
    private static native void queryPathString(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long resultPtr, long destPtr, int maxSize);
    private static native boolean queryPathBoolean(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long resultPtr);
    private static native long queryPathLong(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long resultPtr);
    private static native double queryPathDouble(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long resultPtr);

    /** Get a path and force the result to a string, regardless of the type. */
    public static void queryPath(DirectUtf8Sink json, DirectUtf8Sequence path, JsonResult result, DirectUtf8Sink dest, int maxSize) {
        try (NativeByteSink nativeDest = dest.borrowDirectByteSink()) {
            queryPath(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), result.ptr(), nativeDest.ptr(), maxSize);
        }
    }

    /** Extract a string path. If it's not a string it will error. */
    public static void queryPathString(DirectUtf8Sink json, DirectUtf8Sequence path, JsonResult result, DirectUtf8Sink dest, int maxSize) {
        try (NativeByteSink nativeDest = dest.borrowDirectByteSink()) {
            queryPathString(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), result.ptr(), nativeDest.ptr(), maxSize);
        }
    }

    /** Extract a boolean path. If it's not a boolean it will error. */
    public static boolean queryPathBoolean(DirectUtf8Sink json, DirectUtf8Sequence path, JsonResult result) {
        return queryPathBoolean(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), result.ptr());
    }

    /** Extract a long path. If it's not a long it will error. */
    public static long queryPathLong(DirectUtf8Sink json, DirectUtf8Sequence path, JsonResult result) {
        return queryPathLong(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), result.ptr());
    }

    /** Extract a double path. If it's not a double it will error. */
    public static double queryPathDouble(DirectUtf8Sink json, DirectUtf8Sequence path, JsonResult result) {
        return queryPathDouble(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), result.ptr());
    }

    /** Validate the document and return a error code from the `JsonError` class's constants. */
    public static int validate(DirectUtf8Sink json) {
        return validate(json.ptr(), json.size(), json.capacity());
    }

    private native static int getSimdJsonPadding();

    static {
        Os.init();
        SIMDJSON_PADDING = getSimdJsonPadding();
        JsonError.init();
    }
}
