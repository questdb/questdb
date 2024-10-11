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

package io.questdb.cairo;


import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public interface CairoMetadataRW extends CairoMetadataRO {
    void clear();

    void dropTable(@NotNull TableToken tableToken);

    void dropTable(@NotNull CharSequence tableName);

    @TestOnly
    void hydrateAllTables();

    void hydrateTable(@NotNull CharSequence tableName, boolean infoLog);

    /**
     * Hydrates table metadata, bypassing TableWriter/Reader. Uses a thread-local Path/ColumnVersionReader
     * <p>
     * This function reads the table metadata from file directly, bypassing TableReader/Writer. This ensures that it is
     * non-blocking.
     * <p>
     * One must be careful on the setting of the `blindUpsert` value. When set to true, the data will be blindly
     * upserted to the tables list, which could clobber any concurrent metadata update, leading to inconsistent state.
     * <p>
     * In general, any metadata change that does not originate from TableWriter (or friends) should use `blindUpsert=false`.
     *
     * @param token The table token for the table to read metadata.
     */
    void hydrateTable(@NotNull TableToken token, @NotNull Path path, @NotNull ColumnVersionReader columnVersionReader, boolean infoLog);

    void hydrateTable(@NotNull TableWriterMetadata tableMetadata, boolean infoLog);

    void hydrateTable(@NotNull TableToken token, boolean infoLog);
}
