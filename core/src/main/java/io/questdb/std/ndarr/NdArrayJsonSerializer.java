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

package io.questdb.std.ndarr;

import io.questdb.cairo.ColumnType;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class NdArrayJsonSerializer {
    private NdArrayJsonSerializer() {
    }

    public static void serialize(
            @NotNull CharSink<?> sink,
            @NotNull NdArrayView array,
            @NotNull NdArrayRowMajorTraversal traversal,
            int columnType) {
        assert ColumnType.isNdArray(columnType);
        if (array.isNull()) {
            sink.putAscii("[]");
            return;
        }
        traversal.of(array);
        DirectIntSlice coordinates;
        while ((coordinates = traversal.next()) != null) {
            writeRepeated(sink, traversal.getIn(), '[');
            writeElement(sink, array, coordinates, columnType);
            writeRepeated(sink, traversal.getOut(), ']');
            if (traversal.hasNext()) {
                sink.putAscii(',');
            }
        }
    }

    private static void writeElement(CharSink<?> sink, @NotNull NdArrayView array, DirectIntSlice coordinates, int columnType) {
        final int precision = ColumnType.getNdArrayElementTypePrecision(columnType);
        final char typeClass = ColumnType.getNdArrayElementTypeClass(columnType);
        switch (precision) {
            case 0:
                assert typeClass == 'u';
                final boolean boolVal = array.getBoolean(coordinates);
                sink.putAscii(boolVal ? "true" : "false");
                break;
            case 3:
                assert typeClass == 'i';
                final byte byteVal = array.getByte(coordinates);
                sink.put(byteVal);
                break;
            case 4:
                assert typeClass == 'i';
                final short shortVal = array.getShort(coordinates);
                sink.put(shortVal);
                break;
            case 5:
                switch (typeClass) {
                    case 'i':
                        final int intVal = array.getInt(coordinates);
                        sink.put(intVal);
                        break;
                    case 'f':
                        final float floatVal = array.getFloat(coordinates);
                        sink.put(floatVal);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + precision + typeClass);
                }
                break;
            case 6:
                switch (typeClass) {
                    case 'i':
                        final long longVal = array.getLong(coordinates);
                        sink.put(longVal);
                        break;
                    case 'f':
                        final double doubleVal = array.getDouble(coordinates);
                        sink.put(doubleVal);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + precision + typeClass);
                }
                break;

            default:
                throw new UnsupportedOperationException("Unsupported type: " + precision + typeClass);
        }
    }

    private static void writeRepeated(@NotNull CharSink<?> sink, int count, char ch) {
        for (int i = 0; i < count; ++i) {
            sink.putAscii(ch);
        }
    }
}
