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
        queryPathString(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), dest.borrowDirectByteSink().ptr());
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

    public static void main(String[] args) throws JsonException {
        final String jsonStr = "{\n" +
                "  \"name\": \"John\",\n" +
                "  \"age\": 30,\n" +
                "  \"city\": \"New York\",\n" +
                "  \"hasChildren\": false,\n" +
                "  \"height\": 5.6,\n" +
                "  \"nothing\": null,\n" +
                "  \"pets\": [\n" +
                "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
                "    {\"name\": \"Whiskers\", \"species\": \"Cat\"}\n" +
                "  ]\n" +
                "}";
        DirectUtf8Sink sink = new DirectUtf8Sink(jsonStr.length());

        System.out.println(jsonStr);
        sink.put(jsonStr);

        validate(sink);

        String strPath = ".name";
        DirectUtf8Sink dest = new DirectUtf8Sink(64);
        queryPathString(sink, new GcUtf8String(strPath), dest);
        System.out.println(strPath + ": " + dest);

        String booleanPath = ".hasChildren";
        System.out.println(booleanPath + ": " + queryPathBoolean(sink, new GcUtf8String(booleanPath)));

        String longPath = ".age";
        System.out.println(longPath + ": " + queryPathLong(sink, new GcUtf8String(longPath)));

        String doublePath = ".height";
        System.out.println(doublePath + ": " + queryPathDouble(sink, new GcUtf8String(doublePath)));

        dest.clear();
        String invalidPath = "£$£%£%invalid path!!";
        try {
            GcUtf8String str = new GcUtf8String(invalidPath);
            queryPathString(sink, str, dest);
        } catch (JsonException e) {
            System.err.println(e);
        }

        sink.close();
        dest.close();

    }
}
