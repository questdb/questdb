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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.io;

import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.std.*;
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

    public void parse() {
        map.clear();

        Misc.urlDecode(address, wptr, map, csPool);

        for (int i = 0, n = map.size(); i < n; i++) {
            map.keys().getQuick(i);
            CharSequence name = map.keys().getQuick(i);
            int ordinal = ImportedColumnTypeUtil.LOOKUP.get(map.get(name));
            if (ordinal > -1) {
                ImportedColumnMetadata m = mPool.next();
                m.name = name;
                m.type = ImportedColumnType.values()[ordinal];
                metadata.add(m);
            }
        }
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
