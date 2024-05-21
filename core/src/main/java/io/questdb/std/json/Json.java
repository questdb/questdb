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
    private static final int SIMDJSON_PADDING;

    private static native void validate(long s, long len, long capacity) throws JsonException;

    public static native void queryPathString(long jsonPtr, long jsonLen, long jsonCapacity, long pathPtr, long pathLen, long dest) throws JsonException;

    public static void queryPathString(DirectUtf8Sink json, DirectUtf8Sequence path, DirectUtf8Sink dest) throws JsonException {
        queryPathString(json.ptr(), json.size(), json.capacity(), path.ptr(), path.size(), dest.borrowDirectByteSink().ptr());
    }

    public static void validate(DirectUtf8Sink json) throws JsonException{
        validate(json.ptr(), json.size(), json.capacity());
    }

    private native static int getSimdJsonPadding();

    static {
        Os.init();
        SIMDJSON_PADDING = getSimdJsonPadding();
    }

    public static void main(String[] args) {
        DirectUtf8Sink sink = new DirectUtf8Sink(64);
        System.err.println(SIMDJSON_PADDING);
        final String t1 = "{\n" +
                "  \"name\": \"John\",\n" +
                "  \"age\": 30,\n" +
                "  \"city\": \"New York\",\n" +
                "  \"pets\": [\n" +
                "    {\"name\": \"Max\", \"species\": \"Dog\"},\n" +
                "    {\"name\": \"Whiskers\", \"species\": \"Cat\"}\n" +
                "  ]\n" +
                "}";
//        final String t1 = "[\n" +
//                "  { \"make\": \"Toyota\", \"model\": \"Camry\",  \"year\": 2018, \"tire_pressure\": [ 40.1, 39.9, 37.7, 40.4 ] },\n" +
//                "  { \"make\": \"Kia\",    \"model\": \"Soul\",   \"year\": 2012, \"tire_pressure\": [ 30.1, 31.0, 28.6, 28.7 ] },\n" +
//                "  { \"make\": \"Toyota\", \"model\": \"Tercel\", \"year\": 1999, \"tire_pressure\": [ 29.8, 30.0, 30.2, 30.5 ] }\n" +
//                "]";
        sink.put(t1);

        validate(sink);

        String path = ".name";
//        String path = ".pets[1].species";
//        String path = "[0].make";
        DirectUtf8Sink dest = new DirectUtf8Sink(64);
        queryPathString(sink, new GcUtf8String(path), dest);
        System.out.println(dest);
    }
}
