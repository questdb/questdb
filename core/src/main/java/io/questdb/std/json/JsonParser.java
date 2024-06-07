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
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;

public class JsonParser implements QuietCloseable {
    public static final int SIMDJSON_PADDING;
    private long impl;

    public JsonParser() {
        impl = create();
    }

    /**
     * Convert a JSON path to the SIMDJSON pointer format.
     */
    public static void convertJsonPathToPointer(DirectUtf8Sequence path, DirectUtf8Sink dest) {
        try (NativeByteSink destSink = dest.borrowDirectByteSink()) {
            if (path.size() > 0) {
                convertJsonPathToPointer(path.ptr(), path.size(), destSink.ptr());
            }
        }
    }

    @Override
    public void close() {
        if (impl != 0) {
            destroy(impl);
            impl = 0;
        }
    }

    /**
     * Get a pointer value and force the result to a string, regardless of the type.
     */
    public void queryPointer(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result, DirectUtf8Sink dest, int maxSize) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        final long nativeByteSinkPtr = dest.borrowDirectByteSink().ptr();
        assert dest.capacity() - dest.size() >= maxSize;  // Without this guarantee we'd need to close `NativeByteSink.close`.
        queryPointer(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr(), nativeByteSinkPtr, maxSize);
    }

    public boolean queryPointerBoolean(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerBoolean(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public double queryPointerDouble(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerDouble(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public float queryPointerFloat(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerFloat(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public int queryPointerInt(DirectUtf8Sequence json, DirectUtf8Sink pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerInt(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public long queryPointerLong(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerLong(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public short queryPointerShort(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerShort(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr());
    }

    public void queryPointerString(DirectUtf8Sequence json, DirectUtf8Sequence pointer, JsonResult result, DirectUtf8Sink dest, int maxSize) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        final long nativeByteSinkPtr = dest.borrowDirectByteSink().ptr();
        assert dest.capacity() - dest.size() >= maxSize;  // Without this guarantee we'd need to close `NativeByteSink.close`.
        queryPointerString(impl, json.ptr(), json.size(), json.tailPadding(), pointer.ptr(), pointer.size(), result.ptr(), nativeByteSinkPtr, maxSize);
    }

    private static native void convertJsonPathToPointer(
            long pathPtr,
            long pathLen,
            long destPtr
    );

    private static native long create();

    private static native void destroy(long impl);

    private native static int getSimdJsonPadding();

    private static native void queryPointer(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr,
            long destPtr,
            int maxSize
    );

    private static native boolean queryPointerBoolean(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native double queryPointerDouble(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native float queryPointerFloat(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native int queryPointerInt(
            long impl,
            long jsonPtr,
            int jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native long queryPointerLong(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native short queryPointerShort(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr
    );

    private static native void queryPointerString(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            long pointerPtr,
            long pointerLen,
            long resultPtr,
            long destPtr,
            int maxSize
    );

    static {
        Os.init();
        SIMDJSON_PADDING = getSimdJsonPadding();
        JsonError.init();
    }
}
