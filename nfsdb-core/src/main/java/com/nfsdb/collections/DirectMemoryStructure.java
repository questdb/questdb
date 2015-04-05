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

package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;

import java.io.Closeable;

public class DirectMemoryStructure implements Closeable {

    protected long address;

    @Override
    public void close() {
        free();
    }

    public void free() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;
            freeInternal();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    protected void freeInternal() {

    }
}
