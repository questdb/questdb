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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public interface TableNameRegistry extends Closeable {
    TableToken LOCKED_TOKEN = new TableToken("__locked__", "__locked__", Integer.MAX_VALUE, false);

    /**
     * cleans the registry and releases all resources
     */
    void close();

    /**
     * Returns table name by table token. If table does not exist, returns null.
     *
     * @param token table token
     * @return table name or throws exception if table does not exist
     */
    @NotNull String getTableName(TableToken token);

    /**
     * Returns table token by table name. If table does not exist, returns null.
     *
     * @return resolves table name to TableToken. If no token exists, returns null
     */
    TableToken getTableToken(CharSequence tableName);

    /**
     * Returns table token by directory name. If table does not exist, returns null.
     *
     * @return resolves private table name to TableToken. If no token exists, returns null
     */
    TableToken getTableToken(String dirName, int tableId);

    /**
     * Returns all table tokens. Among live table it also returns dropped tables which are not fully deleted yet.
     *
     * @return all table tokens
     */
    Iterable<TableToken> getTableTokens();

    /**
     * Checks that table token does not belong to a dropped table.
     *
     * @param tableToken table token to check
     * @return true if table id dropped, false otherwise
     */
    boolean isTableDropped(TableToken tableToken);

    /**
     * Locks table name for creation and returns table token.
     * {@link #registerName(TableToken)} must be called to complete table creation and release the lock.
     * or {@link #unlockTableName(TableToken)} must be called to release lock without completing table creation.
     *
     * @param tableName        table name
     * @param privateTableName private table name, e.g. the directory where the table files are stored
     * @param tableId          unique table id
     * @param isWal            true if table is WAL enabled
     * @return table token or null if table name with the same tableId, private name is already registered
     */
    TableToken lockTableName(String tableName, String privateTableName, int tableId, boolean isWal);

    /**
     * Returns most up-to-date table token, including updated Table Logging Name. If table does not exist, returns null.
     *
     * @return resolved TableToken. If no token exists, returns null
     */
    TableToken refreshTableToken(TableToken TableToken);

    /**
     * Registers table name and releases lock. This method must be called after {@link #lockTableName(String, String, int, boolean)}.
     *
     * @param tableToken table token returned by {@link #lockTableName(String, String, int, boolean)}
     */
    void registerName(TableToken tableToken);


    /**
     * Reloads table name registry from storage.
     */
    void reloadTableNameCache();

    /**
     * Part of "drop table" workflow. Purges name from the registry to make the name available for new
     * tables. The method checks that table name and token both match the latest information held in
     * the registry. This is to avoid race condition on "drop" and "create" table by the same name. 
     *
     * @param tableName  name of the table
     * @param token table token of the same table as of the time of "drop"
     * @return true if table name was removed, false otherwise
     */
    boolean dropTable(CharSequence tableName, TableToken token);

    /**
     * Purges token from registry after table, and it's WAL segments have been removed on disk. This method is
     * part of async directory purging job.
     *
     * @param token table token to remove
     */
    void purgeToken(TableToken token);

    /**
     * Updates table name in registry.
     *
     * @param oldName    old table name
     * @param newName    new table name
     * @param tableToken table token to make sure intended table name is updated
     * @return updated table token
     */
    TableToken rename(CharSequence oldName, CharSequence newName, TableToken tableToken);

    /**
     * Resets table name storage memory to initial value. Used to not false detect memory leaks in tests.
     */
    @TestOnly
    void resetMemory();

    /**
     * Unlocks table name. This method must be called after {@link #lockTableName(String, String, int, boolean)}.
     * If table name is not locked, does nothing.
     *
     * @param tableToken table token returned by {@link #lockTableName(String, String, int, boolean)}
     */
    void unlockTableName(TableToken tableToken);
}
