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

package io.questdb.std;

public class DirectByteSequenceView implements BinarySequence, Mutable {
    private long address;
    private long len = -1;

    @Override
    public byte byteAt(long index) {
        return Unsafe.getUnsafe().getByte(address + index);
    }

    @Override
    public void clear() {
        len = -1;
    }

    @Override
    public void copyTo(long address, final long start, final long length) {
        long bytesRemaining = Math.min(length, this.len - start);
        long addr = this.address + start;
        Vect.memcpy(address, addr, bytesRemaining);
    }

    @Override
    public long length() {
        return len;
    }

    public DirectByteSequenceView of(long address, long len) {
        this.address = address;
        this.len = len;
        return this;
    }

    public DirectByteSequenceView of(DirectByteSequenceView other) {
        this.address = other.address;
        this.len = other.len;
        return this;
    }
}
