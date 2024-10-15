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


import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public interface MetadataCacheWriter extends QuietCloseable {
    void clearCache();

    void dropTable(@NotNull TableToken tableToken);

    /**
     * Updates the entire cache sourcing table names from the Engine. The previous
     * state of the cache is discarded
     */
    void hydrateAllTables();

    /**
     * Updates the metadata cache using table name. If name is invalid
     * the method will throw TableReferenceOutOfDateException.
     *
     * @param tableName The name of the table to read metadata.
     * @throws io.questdb.cairo.sql.TableReferenceOutOfDateException if the table name is invalid
     */
    void hydrateTable(@NotNull CharSequence tableName) throws TableReferenceOutOfDateException;

    /**
     * Updates the metadata cache with new entry for the given table. The entry values are
     * read from the _meta file.
     *
     * @param token The table token for the table to read metadata.
     */
    void hydrateTable(@NotNull TableToken token);

    /**
     * Updates the metadata cache with new entry, which content is provided by
     * the given {@link TableWriterMetadata} instance. This metadata is typically sourced from
     * a TableWriter instance, which is the source of truth for table metadata.
     *
     * @param tableMetadata The metadata to update the cache with.
     */
    void hydrateTable(@NotNull TableWriterMetadata tableMetadata);

    void renameTable(@NotNull TableToken fromTableToken, @NotNull TableToken toTableToken);
}
