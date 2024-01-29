/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * CRC32C-based hash function. Should be preferred on x86 due to AVX2 intrinsics
 * used in the standard CRC32C class.
 * <p>
 * Note: CRC32C class is available in Java 9+.
 */
public class Crc32CFunction implements Hash64Function {
    private static final long ADDRESS_FIELD_OFFSET;
    private static final long CAPACITY_FIELD_OFFSET;
    private static final long LIMIT_FIELD_OFFSET;

    // We use this ByteBuffer as a workaround for calling CRC32C over native memory.
    private final ByteBuffer buf = ByteBuffer.allocateDirect(0);
    private final CRC32C crc = new CRC32C();

    @Override
    public void close() {
        // reset to dummy buffer to avoid double free
        resetBufferToPointer(0, 0);
    }

    @Override
    public long hash(long p, long len) {
        resetBufferToPointer(p, len);
        crc.reset();
        crc.update(buf);
        return crc.getValue();
    }

    private void resetBufferToPointer(long ptr, long len) {
        Unsafe.getUnsafe().putLong(buf, ADDRESS_FIELD_OFFSET, ptr);
        Unsafe.getUnsafe().putLong(buf, LIMIT_FIELD_OFFSET, len);
        Unsafe.getUnsafe().putLong(buf, CAPACITY_FIELD_OFFSET, len);
        buf.position(0);
    }

    static {
        Field addressField;
        Field limitField;
        Field capacityField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            limitField = Buffer.class.getDeclaredField("limit");
            capacityField = Buffer.class.getDeclaredField("capacity");
        } catch (NoSuchFieldException e) {
            // possible improvement: implement a fallback strategy when reflection is unavailable for any reason.
            throw new ExceptionInInitializerError(e);
        }
        ADDRESS_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
        LIMIT_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(limitField);
        CAPACITY_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(capacityField);
    }
}
