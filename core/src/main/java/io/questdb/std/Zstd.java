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

package io.questdb.std;

/**
 * JNI wrapper over libzstd, used by the QWP egress protocol to compress
 * {@code RESULT_BATCH} frames. Contexts are long-lived: a {@code CCtx} is
 * allocated once per connection on the server and a {@code DCtx} once per
 * IoThread on the client, both reused across every batch.
 * <p>
 * All methods operate on raw native addresses so callers can pass direct
 * buffer pointers with zero copies. The native implementation lives in
 * {@code core/rust/qdbr/src/qwp_zstd.rs} and is packaged inside libquestdbr.
 */
public final class Zstd {

    private Zstd() {
    }

    /**
     * Compresses {@code srcLen} bytes at {@code srcAddr} into the buffer at
     * {@code dstAddr} (capacity {@code dstCap}). Returns the number of bytes
     * written on success; a negative value encodes a zstd error code.
     */
    public static native long compress(long ctx, long srcAddr, long srcLen, long dstAddr, long dstCap);

    public static native long createCCtx(int level);

    public static native long createDCtx();

    public static native long decompress(long ctx, long srcAddr, long srcLen, long dstAddr, long dstCap);

    public static native void freeCCtx(long ptr);

    public static native void freeDCtx(long ptr);
}
