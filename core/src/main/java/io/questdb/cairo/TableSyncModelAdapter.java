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

package io.questdb.cairo;

final class TableSyncModelAdapter implements TableStructure {
    private final TableSyncModel model;

    public TableSyncModelAdapter(TableSyncModel model) {
        this.model = model;
    }

    @Override
    public int getColumnCount() {
        return model.getAddedColumnSize();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return model.getAddedColumnMetadata(columnIndex).getName();
    }

    @Override
    public int getColumnType(int columnIndex) {
        return model.getAddedColumnMetadata(columnIndex).getType();
    }

    @Override
    public long getColumnHash(int columnIndex) {
        return model.getAddedColumnMetadata(columnIndex).getHash();
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return model.getAddedColumnMetadata(columnIndex).getIndexValueBlockCapacity();
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return model.getAddedColumnMetadata(columnIndex).isIndexed();
    }

    @Override
    public boolean isSequential(int columnIndex) {
        //todo: this is a reserved flag. we should replicate it neverthless.
        return false;
    }

    @Override
    public int getPartitionBy() {
        return model.getPartitionBy();
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return model.getAddedColumnSymbolCache(columnIndex);
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return model.getAddedColumnSymbolCapacity(columnIndex);
    }

    @Override
    public CharSequence getTableName() {
        return model.getTableName();
    }

    @Override
    public int getTimestampIndex() {
        return model.getTimestampIndex();
    }

    @Override
    public int getMaxUncommittedRows() {
        return model.getMaxUncommittedRows();
    }

    @Override
    public long getCommitLag() {
        return model.getCommitLag();
    }
}
