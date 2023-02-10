/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public final class VarInt {
    // 1 byte carries 7 bits of data
    // the highest bit is used to indicate if this is the last byte or more to follow
    // this codec deals only with positive values

    public static long encode(long value, long bufferPos, long bufHi) {
        if (value < 0) {
            throw new RuntimeException("Positive value expected [value=" + value + "]");
        }

        while (value > 0x7F) {
            if (bufferPos >= bufHi) {
                throw new RuntimeException("Buffer overflow [bufferPos=" + bufferPos + ", bufHi=" + bufHi + "]");
            }
            Unsafe.getUnsafe().putByte(bufferPos++, (byte) (value & 0x7F | 0x80));
            value >>= 7;
        }
        Unsafe.getUnsafe().putByte(bufferPos++, (byte) value);

        return bufferPos;
    }

    public static long decode(Value holder, long bufferPos, long bufHi) {
        if (bufferPos >= bufHi) {
            throw new RuntimeException("Buffer overflow [bufferPos=" + bufferPos + ", bufHi=" + bufHi + "]");
        }
        byte b = Unsafe.getUnsafe().getByte(bufferPos++);

        int i = 0;
        long value = 0;
        while ((b & 0x80) == 0x80) {
            value += (long)(b & 0x7f) << i++ * 7;
            if (bufferPos >= bufHi) {
                throw new RuntimeException("Buffer overflow [bufferPos=" + bufferPos + ", bufHi=" + bufHi + "]");
            }
            b = Unsafe.getUnsafe().getByte(bufferPos++);
        }
        value += (long)(b & 0x7f) << i * 7;

        holder.value = value;
        return bufferPos;
    }

    public static class Value {
        private long value;

        public long get() {
            return value;
        }
    }
}
