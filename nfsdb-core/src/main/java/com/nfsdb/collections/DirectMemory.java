/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import sun.misc.Cleaner;

import java.io.Closeable;

public class DirectMemory implements Closeable {

    private final Cleaner cleaner = Cleaner.create(this, new Runnable() {
        @Override
        public void run() {
            free0();
        }
    });
    protected long address;

    private void free0() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;

            freeInternal();
        }
    }

    protected void freeInternal() {

    }

    public void free() {
        cleaner.clean();
    }

    @Override
    public void close() {
        free();
    }
}
