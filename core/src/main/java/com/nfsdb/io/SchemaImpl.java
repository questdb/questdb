/*******************************************************************************
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
 ******************************************************************************/

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
    private int size = 0;

    public SchemaImpl(ObjectPool<DirectByteCharSequence> csPool, ObjectPool<ImportedColumnMetadata> mPool) {
        this.csPool = csPool;
        this.mPool = mPool;
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

    public Schema of(CharSequence schema) {
        int _sz = schema.length() * 2;
        if (_sz > size) {
            if (address > 0) {
                Unsafe.getUnsafe().freeMemory(address);
            }
            this.address = Unsafe.getUnsafe().allocateMemory(_sz);
            this.size = _sz;
        }
        long ptr = address;

        for (int i = 0; i < schema.length(); i++) {
            Unsafe.getUnsafe().putByte(ptr++, (byte) schema.charAt(i));
        }

        map.clear();

        Misc.urlDecode(address, ptr, map, csPool);

        for (int i = 0, n = map.size(); i < n; i++) {
            map.keys().getQuick(i);

            ImportedColumnMetadata m = mPool.next();
            m.name = map.keys().getQuick(i);
            m.type = ImportedColumnType.valueOf(map.get(m.name).toString());
            metadata.add(m);
        }

        return this;
    }
}
