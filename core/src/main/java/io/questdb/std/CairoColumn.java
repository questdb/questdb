/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.TableColumnMetadata;
import org.jetbrains.annotations.NotNull;

public class CairoColumn {
    private boolean designated;
    private int indexBlockCapacity;
    private boolean isDedupKey;
    private boolean isIndexed;
    private SimpleReadWriteLock lock;
    private CharSequence name;
    private int position;
    private boolean symbolCached;
    private int symbolCapacity;
    private int type;

    public CairoColumn() {
        this.lock = new SimpleReadWriteLock();
    }

    public CairoColumn(@NotNull TableColumnMetadata metadata, boolean designated, int position) {
        this.lock = new SimpleReadWriteLock();
        updateMetadata(metadata, designated, position);
    }

    public void copyTo(@NotNull CairoColumn target) {
        lock.readLock().lock();
        target.lock.writeLock().lock();

        target.indexBlockCapacity = this.indexBlockCapacity;
        target.isIndexed = this.isIndexed;
        target.name = this.name;
        target.type = this.type;
        target.designated = this.designated;
        target.position = this.position;
        target.symbolCached = this.symbolCached;
        target.symbolCapacity = this.symbolCapacity;
        target.isDedupKey = this.isDedupKey;

        target.lock.writeLock().unlock();
        lock.readLock().unlock();
    }

    public boolean getDedupKey() {
        lock.readLock().lock();
        final boolean upsertKey = this.isDedupKey;
        lock.readLock().unlock();
        return upsertKey;
    }

    public boolean getDesignated() {
        lock.readLock().lock();
        final boolean designated = this.designated;
        lock.readLock().unlock();
        return designated;
    }

    public boolean getDesignatedUnsafe() {
        return designated;
    }

    public int getIndexBlockCapacity() {
        lock.readLock().lock();
        final int indexBlockCapacity = this.indexBlockCapacity;
        lock.readLock().unlock();
        return indexBlockCapacity;
    }

    public int getIndexBlockCapacityUnsafe() {
        return indexBlockCapacity;
    }

    public boolean getIndexed() {
        lock.readLock().lock();
        final boolean indexed = this.isIndexed;
        lock.readLock().unlock();
        return indexed;
    }

    public boolean getIndexedUnsafe() {
        return isIndexed;
    }

    public CharSequence getName() {
        lock.readLock().lock();
        final CharSequence name = this.name;
        lock.readLock().unlock();
        return name;
    }

    public CharSequence getNameUnsafe() {
        return name;
    }

    public int getPosition() {
        lock.readLock().lock();
        final int position = this.position;
        lock.readLock().unlock();
        return position;
    }

    public int getPositionUnsafe() {
        return position;
    }

    public boolean getSymbolCached() {
        lock.readLock().lock();
        final boolean symbolCached = this.symbolCached;
        lock.readLock().unlock();
        return symbolCached;
    }

    public boolean getSymbolCachedUnsafe() {
        return symbolCached;
    }

    public int getSymbolCapacity() {
        lock.readLock().lock();
        final int symbolCapacity = this.symbolCapacity;
        lock.readLock().unlock();
        return symbolCapacity;
    }

    public int getSymbolCapacityUnsafe() {
        return symbolCapacity;
    }

    public int getType() {
        lock.readLock().lock();
        final int type = this.type;
        lock.readLock().unlock();
        return type;
    }

    public int getTypeUnsafe() {
        return type;
    }

    public boolean getUpsertKeyUnsafe() {
        return isDedupKey;
    }

    public void updateMetadata(@NotNull TableColumnMetadata tableColumnMetadata, boolean designated, int position) {
        lock.writeLock().lock();

        name = tableColumnMetadata.getName();
        type = tableColumnMetadata.getType();
        isDedupKey = tableColumnMetadata.isDedupKey();
        isIndexed = tableColumnMetadata.isIndexed();
        indexBlockCapacity = tableColumnMetadata.getIndexValueBlockCapacity();
        this.designated = designated;
        this.position = position;


//        private boolean symbolCached;
//        private int symbolCapacity;


        lock.writeLock().unlock();

    }
}
