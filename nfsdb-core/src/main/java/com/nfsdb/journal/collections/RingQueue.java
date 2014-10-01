/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.collections;

import java.lang.reflect.Array;

public class RingQueue<T> {
    private final Class<T> c;
    private T buffer[];
    private int len;
    private long writeStart;
    private long writePos;
    private long readPos;
    private boolean init = true;

    @SuppressWarnings("unchecked")
    public RingQueue(Class<T> c, int len) {
        this.len = len;
        this.c = c;
        this.buffer = (T[]) Array.newInstance(c, len);
        this.writePos = 0;
        this.writeStart = 0;
        this.readPos = 0;
    }

    public long nextWritePos() {
        if (writePos % len == writeStart % len) {
            if (!init) {
                resize();
            }
            init = false;
        }

        return writePos++;
    }

    public T get(long pos) {
        return buffer[((int) (pos % len))];
    }

    public void put(long pos, T value) {
        buffer[((int) (pos % len))] = value;
    }

    @SuppressWarnings("unchecked")
    private void resize() {
        int newLen = len * 2;

        int pivot = (int) (writeStart % len);

        T buf[] = (T[]) Array.newInstance(c, newLen);
        System.arraycopy(buffer, 0, buf, 0, pivot);
        System.arraycopy(buffer, pivot, buf, len + pivot, len - pivot);

        // correct formula is
        // x = x + (x/oldLen) * (newLen - oldLen)
        // this is short version because newLen=2*oldLen

        readPos += (readPos / len) * len;
        // if readPos is after writeStart shift it
        // just like writeStart
        if (readPos % len >= pivot) {
            readPos += len;
        }

        writePos += (writePos / len) * len;
        writeStart += (writeStart / len) * len + len;

        this.buffer = buf;
        this.len = newLen;
    }

    public boolean hasNext() {
        return readPos < writePos;
    }

    public T next() {
        return buffer[((int) (readPos++ % len))];
    }

    public void mark() {
        writeStart = readPos - 1;
    }

    public void toMark() {
        readPos = writeStart;
    }

    @Override
    public String toString() {
        return "RingQueue{" +
                "len=" + len +
                ", writeStart=" + writeStart +
                ", writePos=" + writePos +
                ", readPos=" + readPos +
                ", init=" + init +
                '}';
    }
}
