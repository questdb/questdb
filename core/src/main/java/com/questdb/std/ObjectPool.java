/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Single threaded object pool based on ObjList. The goal is to optimise intermediate allocation of intermediate objects.
 */
public class ObjectPool<T extends Mutable> implements Mutable {

    private final static Log LOG = LogFactory.getLog(ObjectPool.class);
    private final ObjList<T> list;
    private final ObjectFactory<T> factory;
    private int pos = 0;
    private int size;

    public ObjectPool(@NotNull ObjectFactory<T> factory, int size) {
        this.list = new ObjList<>(size);
        this.factory = factory;
        this.size = size;
        fill();
    }

    @Override
    public void clear() {
        pos = 0;
    }

    public T next() {
        if (pos == size) {
            expand();
        }

        T o = list.getQuick(pos++);
        o.clear();
        return o;
    }

    private void expand() {
        fill();
        size <<= 1;
        LOG.info().$("pool resize [class=").$(factory.getClass().getName()).$(", size=").$(size).$(']').$();
    }

    private void fill() {
        for (int i = 0; i < size; i++) {
            list.add(factory.newInstance());
        }
    }
}
