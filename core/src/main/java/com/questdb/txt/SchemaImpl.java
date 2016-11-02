/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.txt;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectPool;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class SchemaImpl implements Schema, Closeable, Mutable {

    private final static Log LOG = LogFactory.getLog(SchemaImpl.class);
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
            Unsafe.free(address, hi - address);
            address = 0;
        }
    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return metadata;
    }

    public void parse() {
        map.clear();

        Misc.urlDecode(address, wptr, map, csPool);

        for (int i = 0, n = map.size(); i < n; i++) {
            map.keys().getQuick(i);
            CharSequence name = map.keys().getQuick(i);
            int importedColumnType = ImportedColumnType.importedColumnTypeOf(map.get(name));
            if (importedColumnType > -1) {
                ImportedColumnMetadata m = mPool.next();
                m.name = name;
                m.importedColumnType = importedColumnType;
                metadata.add(m);
            } else {
                LOG.info().$("Unknown column type [").$(map.get(name)).$("] for ").$(name).$();
            }
        }
    }

    public void put(CharSequence cs) {
        int l = cs.length();
        if (wptr + l >= hi) {
            long old_address = this.address;
            long old_wptr = this.wptr;
            long old_size = this.hi - this.address;

            allocate(((int) (hi - address)) + l * 2);
            Unsafe.getUnsafe().copyMemory(old_address, this.address, (old_wptr - old_address));
            this.wptr = this.address + (old_wptr - old_address);
            Unsafe.free(old_address, old_size);
        }
        for (int i = 0; i < cs.length(); i++) {
            Unsafe.getUnsafe().putByte(wptr++, (byte) cs.charAt(i));
        }
    }

    private void allocate(int size) {
        this.address = this.wptr = Unsafe.malloc(size);
        this.hi = this.address + size;
    }
}
