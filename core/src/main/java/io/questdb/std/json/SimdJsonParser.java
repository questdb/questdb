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

package io.questdb.std.json;

import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.NotNull;

public class SimdJsonParser implements QuietCloseable {
    public static final int SIMDJSON_PADDING;
    private long impl;

    public SimdJsonParser() {
        impl = create();
    }

    /**
     * Convert a JSON path to the SIMDJSON pointer format.
     */
    public static void convertJsonPathToPointer(
            @NotNull DirectUtf8Sequence path,
            @NotNull DirectUtf8Sink dest
    ) {
        try (NativeByteSink destSink = dest.borrowDirectByteSink()) {
            convertJsonPathToPointer(path.ptr(), path.size(), destSink.ptr());
        }
    }

    /**
     * Parse the JSON document once and extract all array elements in a single
     * forward pass. Results are written into a pre-sized
     * {@code results[elementCount * columnCount]} matrix. String data
     * (VARCHAR/TIMESTAMP) is appended to the shared string sink, with packed
     * {@code (offset << 32) | length} stored in each result's value field.
     *
     * @param json          padded JSON document
     * @param isObjectArray true if elements are objects, false for scalar arrays
     * @param descsPtr      pointer to native column_desc_t array
     * @param resultsPtr    pointer to native column_result_t matrix (elementCount * columnCount entries)
     * @param columnCount   number of columns
     * @param elementCount  number of array elements to extract
     * @param stringSinkPtr pointer to native questdb_byte_sink_t for all string data
     */
    public void extractAllArrayElements(
            DirectUtf8Sequence json,
            boolean isObjectArray,
            long descsPtr,
            long resultsPtr,
            int columnCount,
            int elementCount,
            long stringSinkPtr
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        extractAllArrayElements(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                json.isAscii(),
                isObjectArray,
                descsPtr,
                resultsPtr,
                columnCount,
                elementCount,
                stringSinkPtr
        );
    }

    @Override
    public void close() {
        if (impl != 0) {
            destroy(impl);
            impl = 0;
        }
    }

    /**
     * Combined array info query and extraction in a single parse. Counts
     * elements, determines object vs scalar type, and if the results buffer
     * is large enough, extracts all elements in one JNI call.
     *
     * @return positive element count if extraction succeeded,
     * negative element count if the buffer was too small (caller
     * should grow the buffer and retry), or 0 for empty/invalid arrays
     */
    public int queryAndExtractArray(
            DirectUtf8Sequence json,
            SimdJsonResult result,
            long descsPtr,
            long resultsPtr,
            int columnCount,
            int resultsCapacity,
            long stringSinkPtr
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryAndExtractArray(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                json.isAscii(),
                result.ptr(),
                descsPtr,
                resultsPtr,
                columnCount,
                resultsCapacity,
                stringSinkPtr
        );
    }

    public boolean queryPointerBoolean(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerBoolean(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr()
        );
    }

    public double queryPointerDouble(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerDouble(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr()
        );
    }

    public int queryPointerInt(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerInt(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr()
        );
    }

    public long queryPointerLong(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerLong(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr()
        );
    }

    public short queryPointerShort(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        return queryPointerShort(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr()
        );
    }

    public void queryPointerUtf8(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result,
            DirectUtf8Sink dest,
            int maxSize
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        final long nativeByteSinkPtr = dest.borrowDirectByteSink().ptr();
        assert dest.capacity() - dest.size() >= maxSize;  // Without this guarantee we'd need to close `NativeByteSink.close`.
        queryPointerUtf8(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                json.isAscii(),
                pointer.ptr(),
                pointer.size(),
                result.ptr(),
                nativeByteSinkPtr,
                maxSize
        );
    }

    /**
     * Generic call that can access any JSON value.
     * Consult the `result`'s `getError()`, `getType()` and `getNumberType()` for processing the data.
     * <p>
     * For example, if the path points to a JSON number extracted as a double:
     * * `result.getError()` will be `SimdJsonError.SUCCESS`
     * * `result.getType()` will be `SimdJsonType.NUMBER`
     * * `result.getNumberType()` will be `SimdJsonNumberType.FLOATING_POINT_NUMBER`
     * * `queryPointerValue`'s returned long needs to be converted to a double by calling `Double.longBitsToDouble`.
     * <p>
     * On a `null`, the `result.getType()` will be `SimdJsonType.NULL` and returned value 0.
     * <p>
     * On a boolean, the `result.getType()` will be `SimdJsonType.BOOLEAN` and returned value reliably 0 or 1.
     * <p>
     * Arrays and objects are returned as raw JSON strings.
     */
    public long queryPointerValue(
            DirectUtf8Sequence json,
            DirectUtf8Sequence pointer,
            SimdJsonResult result,
            DirectUtf8Sink dest, // IMPORTANT: The `ascii` flag is not set on the dest sink. This is beca
            int maxSize
    ) {
        assert json.tailPadding() >= SIMDJSON_PADDING;
        assert dest.capacity() - dest.size() >= maxSize;  // Without this guarantee we'd need to close `NativeByteSink.close`.
        return queryPointerValue(
                impl,
                json.ptr(),
                json.size(),
                json.tailPadding(),
                pointer.ptr(),
                pointer.size(),
                result.ptr(),
                dest.borrowDirectByteSink().ptr(),
                maxSize
        );
    }

    private static native void convertJsonPathToPointer(
            long pathPtr,
            long pathLen,
            long destPtr
    );

    private static native long create();

    private static native void destroy(long impl);

    private static native void extractAllArrayElements(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            boolean jsonIsAscii,
            boolean isObjectArray,
            long descsPtr,
            long resultsPtr,
            int columnCount,
            int elementCount,
            long stringSinkPtr
    );

    private native static int getSimdJsonPadding();

    private static native int queryAndExtractArray(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            boolean jsonIsAscii,
            long resultPtr,
            long descsPtr,
            long resultsPtr,
            int columnCount,
            int resultsCapacity,
            long stringSinkPtr
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

    private static native int queryPointerInt(
            long impl,
            long jsonPtr,
            long jsonLen,
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

    private static native void queryPointerUtf8(
            long impl,
            long jsonPtr,
            long jsonLen,
            long jsonTailPadding,
            boolean jsonAscii,
            long pointerPtr,
            long pointerLen,
            long resultPtr,
            long destPtr,
            int maxSize
    );

    private static native long queryPointerValue(
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
        SimdJsonError.init();
    }
}
