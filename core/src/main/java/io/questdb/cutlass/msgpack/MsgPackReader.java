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

package io.questdb.cutlass.msgpack;

import java.io.Closeable;
import java.io.IOException;

import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public final class MsgPackReader implements Closeable {

    private static final IllegalStateException BAD_API_CALL = new IllegalStateException("Illegal API call");

    private static final long INIT_CACHE_SIZE = 1048576;  // 1 MiB

    /** void*: A scratch buffer to accumulate the message into. */
    private long cache;

    /** size_t: Allocated size of `cache`. */
    private long cacheCapacity;

    /** void*: Position we've read to into `cache`. */
    private long lo;

    /** void*: Position we've cached to. */
    private long hi;

    /** We've reached the end of the message. */
    private boolean readComplete;

    public MsgPackReader() {
        cache = Unsafe.malloc(INIT_CACHE_SIZE, MemoryTag.NATIVE_DEFAULT);
        cacheCapacity = INIT_CACHE_SIZE;
        lo = 0;
        hi = 0;
        readComplete = false;
    }
    
    /**
     * Prepare to parse the next message.
     * 
     * Any accumulated buffer for the current message will be deleted.
     * Any accumulated buffer for the next message will be retained and moved
     * to the start of the internal cache buffer.
     */
    public void prepareForNextMessage() {
        assert readComplete : "Can only call once reading a message is complete.";
        if (lo < hi) {
            final long outstandingLen = hi - lo;
            Vect.memmove(cache, lo, outstandingLen);
            hi = cache + outstandingLen;
            lo = cache;
            readComplete = false;
        }
    }

    /**
     * Prepare to feed up to `needed` bytes to this reader's internal buffer.
     * Returns a pointer to writable memory.
     * You must follow up with a call to `advance` to inform how much data was actually written.
     * 
     * Here's a common use case for this API:
     *     long ptr = msgPackReader.book(NET_RECV_SIZE);
     *     long actualRead = Net.recv(sockFd, ptr, NET_RECV_SIZE);
     *     checkError(actual);
     *     msgPackReader.advance(actualRead);
     */
    public long book(long needed) {
        // size_t
        final long used = hi - cache;

        // size_t
        final long neededCapacity = used + needed;

        if (neededCapacity > cacheCapacity)
        {
            // size_t
            final long nextCapacity = Numbers.ceilPow2(neededCapacity);
            
            cache = Unsafe.realloc(cache, cacheCapacity, nextCapacity, MemoryTag.NATIVE_DEFAULT);
            cacheCapacity = nextCapacity;
        }

        return hi;
    }

    /** Follow-up call to `book` informing how much data has been written to the reader's buffer. */
    public void advance(long written) {
        hi += written;
    }

    // Number of bytes left to parse available in the accumulated buffer.
    private long remaining() {
        return hi - lo;
    }

    /**
     * Determine the type of the next token. One of `MsgPackType`.
     * 
     * Special values:
     *   * If already reached the end of the message returns 0.
     *   * If more data is required returns -1. Follow-up with call to `book`.
     * 
     * Does not advance. Follow-up with a call to `readXXX`.
     */
    public int next()
    {
        if (readComplete)
            return 0;

        if (remaining() == 0)
            return -1;

        byte leading = Unsafe.getByte(lo);
        return MsgPackTypeEncoding.toType(leading);
    }

    private byte peekByte() {
        if (readComplete || remaining() == 0)
            throw BAD_API_CALL;
        return Unsafe.getByte(lo);
    }

    public void readNil() {
        if (peekByte() == MsgPackTypeEncoding.NIL) {
            lo += 1;
        } else {
            throw BAD_API_CALL;
        }
    }

    public boolean readBool() {
        switch (peekByte()) {
            case MsgPackTypeEncoding.FALSE:
                lo += 1;
                return false;
            case MsgPackTypeEncoding.TRUE:
                lo += 1;
                return true;
            default:
                throw BAD_API_CALL;
        }
    }

    @Override
    public void close() throws IOException {
        Unsafe.free(cache, cacheCapacity, MemoryTag.NATIVE_DEFAULT);
    }
}
