/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.io;

import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

@SuppressFBWarnings({"CLI_CONSTANT_LIST_INDEX"})
public class SchemaImpl implements Schema, Closeable, Mutable {

    private final ObjList<ImportedColumnMetadata> metadata = new ObjList<>();
    private final CharSequenceObjHashMap<CharSequence> map = new CharSequenceObjHashMap<>();
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final ObjectPool<ImportedColumnMetadata> mPool;
    private long address = 0;
    private long hi = 0;
    private long wptr = 0;

    public SchemaImpl(ObjectPool<DirectByteCharSequence> csPool, ObjectPool<ImportedColumnMetadata> mPool) {
        this.csPool = csPool;
        this.mPool = mPool;
        allocate(512);
    }

    @Override
    public void clear() {
        metadata.clear();
    }

    @Override
    public void close() {
        if (address > 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;
        }
    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return metadata;
    }

    public Schema parse() {
        map.clear();

        Misc.urlDecode(address, wptr, map, csPool);

        for (int i = 0, n = map.size(); i < n; i++) {
            map.keys().getQuick(i);

            ImportedColumnMetadata m = mPool.next();
            m.name = map.keys().getQuick(i);
            m.type = ImportedColumnType.valueOf(map.get(m.name).toString());
            metadata.add(m);
        }

        return this;
    }

    public void put(CharSequence cs) {
        int l = cs.length();
        if (wptr + l >= hi) {
            long old_address = this.address;
            long old_wptr = this.wptr;

            allocate(((int) (hi - address)) + l * 2);
            Unsafe.getUnsafe().copyMemory(old_address, this.address, (old_wptr - old_address));
            this.wptr = this.address + (old_wptr - old_address);
            Unsafe.getUnsafe().freeMemory(old_address);
        }
        for (int i = 0; i < cs.length(); i++) {
            Unsafe.getUnsafe().putByte(wptr++, (byte) cs.charAt(i));
        }
    }

    private void allocate(int size) {
        this.address = this.wptr = Unsafe.getUnsafe().allocateMemory(size);
        this.hi = this.address + size;
    }
}
