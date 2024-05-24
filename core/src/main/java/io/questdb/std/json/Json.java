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
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.GcUtf8String;

public class Json {
    public static final int SIMDJSON_PADDING;

    private static native void validate(long s, long len, long capacity) throws JsonException;

    private static native void queryPathString(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long dest) throws JsonException;
    private static native boolean queryPathBoolean(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen) throws JsonException;
    private static native long queryPathLong(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen) throws JsonException;
    private static native double queryPathDouble(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen) throws JsonException;

    public static void queryPathString(DirectUtf8Sink json, DirectUtf8Sequence path, DirectUtf8Sink dest) throws JsonException {
        try (NativeByteSink nativeDest = dest.borrowDirectByteSink()) {
            queryPathString(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), nativeDest.ptr());
        }
    }

    public static boolean queryPathBoolean(DirectUtf8Sink json, DirectUtf8Sequence path) throws JsonException {
        return queryPathBoolean(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size());
    }

    public static long queryPathLong(DirectUtf8Sink json, DirectUtf8Sequence path) throws JsonException {
        return queryPathLong(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size());
    }

    public static double queryPathDouble(DirectUtf8Sink json, DirectUtf8Sequence path) throws JsonException {
        return queryPathDouble(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size());
    }

    public static void validate(DirectUtf8Sink json) throws JsonException{
        validate(json.ptr(), json.size(), json.capacity());
    }

    private native static int getSimdJsonPadding();

    static {
        Os.init();
        SIMDJSON_PADDING = getSimdJsonPadding();
        JsonException.init();
    }
}
