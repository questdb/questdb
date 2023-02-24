/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

class IOTableUpdateDetailsPool implements Closeable {
    private static final int ABASE;
    private static final int ASHIFT;
    private final Log LOG = LogFactory.getLog(IOTableUpdateDetailsPool.class);
    private final Function<CharSequence, ? extends TableUpdateDetails[]> createNewTableUpdateDetails;
    private final ConcurrentHashMap<TableUpdateDetails[]> pool = new ConcurrentHashMap<>();
    private final int pooledSize;
    private volatile boolean closed = false;

    public IOTableUpdateDetailsPool(int maxSize) {
        pooledSize = maxSize;
        createNewTableUpdateDetails = k -> new TableUpdateDetails[pooledSize];
    }

    @Override
    public void close() throws IOException {
        closed = true;
        for (CharSequence key : pool.keySet()) {
            final TableUpdateDetails[] tudArray = pool.get(key);
            for (int n = tudArray.length, i = 0; i < n; i++) {
                TableUpdateDetails tud = getVolatile(tudArray, i);
                if (tud != null) {
                    releaseTud(tud);
                }
            }
        }
    }

    public void closeIdle(long nowMillis, long idleTimeout) {
        for (CharSequence key : pool.keySet()) {
            final TableUpdateDetails[] tudArray = pool.get(key);
            if (tudArray != null) {
                for (int n = tudArray.length, i = 0; i < n; i++) {
                    TableUpdateDetails tud = getVolatile(tudArray, i);
                    if (tud != null && nowMillis - tud.getLastMeasurementMillis() >= idleTimeout) {
                        if (casTabAt(tudArray, i, tud, null)) {
                            LOG.info().$("active table going idle [tableName=").$(tud.getTableNameUtf16()).I$();
                            releaseTud(tud);
                        }
                    }
                }
            }
        }
    }

    public TableUpdateDetails get(CharSequence tableNameUtf8) {
        TableUpdateDetails[] tudArray = pool.get(tableNameUtf8);
        if (tudArray != null) {
            for (int n = tudArray.length, i = 0; i < n; i++) {
                TableUpdateDetails tud = getVolatile(tudArray, i);
                if (tud != null && casTabAt(tudArray, i, tud, null)) {
                    return tud;
                }
            }
        }
        return null;
    }

    public void put(CharSequence tableNameUtf8, TableUpdateDetails tud) {
        TableUpdateDetails[] tudArray = pool.computeIfAbsent(tableNameUtf8, createNewTableUpdateDetails);
        while (true) {
            if (closed) {
                releaseTud(tud);
                return;
            }
            // There must be a place to return the tud, arrays are pre-allocate to hold enough for all IO threads
            for (int n = tudArray.length, i = 0; i < n; i++) {
                if (casTabAt(tudArray, i, null, tud)) {
                    return;
                }
            }
        }
    }

    private void releaseTud(TableUpdateDetails tud) {
        Misc.free(tud);
    }

    static boolean casTabAt(TableUpdateDetails[] tab, int i, TableUpdateDetails expected, TableUpdateDetails v) {
        return Unsafe.getUnsafe().compareAndSwapObject(tab, ((long) i << ASHIFT) + ABASE, expected, v);
    }

    static TableUpdateDetails getVolatile(TableUpdateDetails[] tab, int i) {
        return (TableUpdateDetails) Unsafe.getUnsafe().getObjectVolatile(tab, ((long) i << ASHIFT) + ABASE);
    }

    static {
        Class<?> ak = TableUpdateDetails[].class;
        ABASE = Unsafe.getUnsafe().arrayBaseOffset(ak);
        int scale = Unsafe.getUnsafe().arrayIndexScale(ak);
        if ((scale & (scale - 1)) != 0)
            throw new Error("data type scale not a power of two");
        ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
    }
}
