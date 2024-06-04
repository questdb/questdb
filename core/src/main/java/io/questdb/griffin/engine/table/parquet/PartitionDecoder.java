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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class PartitionDecoder implements QuietCloseable {
    private static final long DESCR_SIZE = 3 * Integer.BYTES;
    private long descrAddr;
    private Metadata metadata = new Metadata();

    public PartitionDecoder() {
        this.descrAddr = Unsafe.malloc(DESCR_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        descrAddr = Unsafe.free(descrAddr, DESCR_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    public Metadata readMetadata(Path srcPath) {
        try {
            readMetadata(
                    srcPath.ptr(),
                    srcPath.size(),
                    descrAddr
            );
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not describe partition: [path=").put(srcPath)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
        return metadata;
    }

    private static native void readMetadata(
            long srcPathPtr,
            int srcPathLength,
            long columnNamesPtr
    );

    public class Metadata {

        public int columnCount() {
            return Unsafe.getUnsafe().getInt(descrAddr);
        }

        public int rowCount() {
            return Unsafe.getUnsafe().getInt(descrAddr + 4L);
        }

        public int rowGroupCount() {
            return Unsafe.getUnsafe().getInt(descrAddr + 8L);
        }
    }

    static {
        Os.init();
    }
}
