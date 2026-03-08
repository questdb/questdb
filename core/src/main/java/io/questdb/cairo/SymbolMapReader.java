/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;

/**
 * Gets the properties of a symbol map, whether symbols in a table are
 * cached, what the table symbol capacity is, updating the symbol count of a table.
 */
public interface SymbolMapReader extends StaticSymbolTable {
    int getSymbolCapacity();

    // Mapped memory to the .o file
    MemoryR getSymbolOffsetsColumn();

    // Mapped memory to the .c file
    MemoryR getSymbolValuesColumn();

    /**
     * @return true if symbol table is cached, otherwise false.
     */
    boolean isCached();

    /**
     * @return true if table reader is deleted, otherwise false.
     */
    boolean isDeleted();

    StaticSymbolTable newSymbolTableView();

    /**
     * @param count number of symbols to update symbol table to
     */
    void updateSymbolCount(int count);
}
