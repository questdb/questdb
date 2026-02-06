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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;

/**
 * Symbol table source for HORIZON JOIN queries that routes symbol table lookups
 * to the correct source (master, sequence, or slave) based on column mappings.
 * <p>
 * Column indices in the combined metadata (baseMetadata) are mapped to their
 * source factories using the columnSources and columnIndices arrays.
 */
class HorizonJoinSymbolTableSource implements SymbolTableSource {
    private final int[] columnIndices;
    private final int[] columnSources;
    private SymbolTableSource masterSource;
    private SymbolTableSource slaveSource;

    HorizonJoinSymbolTableSource(int[] columnSources, int[] columnIndices) {
        this.columnSources = columnSources;
        this.columnIndices = columnIndices;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        int source = columnSources[columnIndex];
        int sourceColumnIndex = columnIndices[columnIndex];
        return switch (source) {
            case HorizonJoinRecord.SOURCE_MASTER -> masterSource.getSymbolTable(sourceColumnIndex);
            case HorizonJoinRecord.SOURCE_SLAVE -> slaveSource.getSymbolTable(sourceColumnIndex);
            // Sequence source (long_sequence) doesn't have symbols
            default -> null;
        };
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        int source = columnSources[columnIndex];
        int sourceColumnIndex = columnIndices[columnIndex];
        return switch (source) {
            case HorizonJoinRecord.SOURCE_MASTER -> masterSource.newSymbolTable(sourceColumnIndex);
            case HorizonJoinRecord.SOURCE_SLAVE -> slaveSource.newSymbolTable(sourceColumnIndex);
            // Sequence source (long_sequence) doesn't have symbols
            default -> null;
        };
    }

    void of(SymbolTableSource masterSource, SymbolTableSource slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
    }
}
