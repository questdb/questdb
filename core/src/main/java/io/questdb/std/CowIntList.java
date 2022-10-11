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

import io.questdb.std.str.CharSink;

import java.util.Arrays;

public class CowIntList {

    private static final int DEFAULT_VALUE = -1;
    private volatile int[] data;

    public CowIntList() {
        this(0);
    }

    public CowIntList(int size) {
        int[] data = new int[size];
        Arrays.fill(data, 0, data.length, DEFAULT_VALUE);
        this.data = data;
    }

    public void add(int value) {
        int[] data = this.data;
        int[] dataCopy = new int[data.length + 1];
        System.arraycopy(data, 0, dataCopy, 0, data.length);
        dataCopy[data.length] = value;
        this.data = dataCopy;
    }

    public void extendAndSet(int index, int value) {
        int[] data = this.data;
        int[] dataCopy;
        if (index >= data.length) {
            dataCopy = new int[index + 1];
            System.arraycopy(data, 0, dataCopy, 0, data.length);
            Arrays.fill(dataCopy, data.length, dataCopy.length, DEFAULT_VALUE);
        } else {
            dataCopy = new int[data.length];
            System.arraycopy(data, 0, dataCopy, 0, data.length);
        }
        dataCopy[index] = value;
        this.data = dataCopy;
    }

    public int get(int index) {
        int[] data = this.data;
        assert index < data.length;
        return data[index];
    }

    public void set(int index, int value) {
        int[] data = this.data;
        assert index < data.length;
        data[index] = value;
        int[] dataCopy = new int[data.length];
        System.arraycopy(data, 0, dataCopy, 0, data.length);
        this.data = dataCopy;
    }

    public int size() {
        int[] data = this.data;
        return data.length;
    }

    @Override
    public String toString() {
        CharSink b = Misc.getThreadLocalBuilder();

        int[] data = this.data;
        b.put('[');
        for (int i = 0; i < data.length; i++) {
            if (i > 0) {
                b.put(',');
            }
            b.put(data[i]);
        }
        b.put(']');
        return b.toString();
    }
}