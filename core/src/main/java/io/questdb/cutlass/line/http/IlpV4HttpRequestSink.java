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

package io.questdb.cutlass.line.http;

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.ilpv4.IlpV4OutputSink;
import io.questdb.std.Unsafe;

/**
 * HTTP request buffer implementation of IlpV4OutputSink.
 * <p>
 * This allows the ILP v4 encoder to write directly to the HTTP client's
 * request buffer, avoiding an intermediate copy.
 * <p>
 * Note: The HTTP request buffer can relocate during growth, so we always
 * get addresses fresh from the request rather than caching them.
 */
public class IlpV4HttpRequestSink implements IlpV4OutputSink {

    private final HttpClient.Request request;

    public IlpV4HttpRequestSink(HttpClient.Request request) {
        this.request = request;
    }

    @Override
    public void putByte(byte value) {
        request.put(value);
    }

    @Override
    public void putShort(short value) {
        // HttpClient.Request doesn't have putShort, so we write bytes
        request.put((byte) value);
        request.put((byte) (value >> 8));
    }

    @Override
    public void putInt(int value) {
        request.putInt(value);
    }

    @Override
    public void putLong(long value) {
        request.putLong(value);
    }

    @Override
    public void putFloat(float value) {
        // Write float as 4 bytes (little-endian)
        int bits = Float.floatToRawIntBits(value);
        request.putInt(bits);
    }

    @Override
    public void putDouble(double value) {
        request.putDouble(value);
    }

    @Override
    public void putLongBE(long value) {
        // Big-endian long
        for (int i = 7; i >= 0; i--) {
            request.put((byte) (value >> (i * 8)));
        }
    }

    @Override
    public int position() {
        return request.getContentLength();
    }

    @Override
    public void position(int pos) {
        // HttpClient.Request doesn't support seeking
        // Header patching is done directly via addressAt() + Unsafe
        // This method is a no-op for HTTP
    }

    @Override
    public long addressAt(int offset) {
        // Always get fresh address from request in case buffer relocated
        return request.getContentStart() + offset;
    }

    @Override
    public int capacity() {
        // Return a large value - HttpClient.Request grows automatically
        return Integer.MAX_VALUE;
    }

    @Override
    public void ensureCapacity(int required) {
        // HttpClient.Request manages capacity automatically via its put methods
        // No action needed here
    }

    @Override
    public void putBytes(long srcAddress, int length) {
        // Use putNonAscii which copies from native memory efficiently
        request.putNonAscii(srcAddress, srcAddress + length);
    }

    /**
     * Returns the underlying HTTP request.
     */
    public HttpClient.Request getRequest() {
        return request;
    }
}
