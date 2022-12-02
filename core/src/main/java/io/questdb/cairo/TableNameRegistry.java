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
     * @param tableToken table token
     * @return table name or null if the table does not exist
     */
    String getTableNameByTableToken(TableToken tableToken);

    /**
     * Returns table token by table name. If table does not exist, returns null.
     *
     * @return resolves table name to TableToken. If no token exists, returns null
     */
    TableToken getTableToken(CharSequence tableName);

    /**
     * Returns table token by private table name. If table does not exist, returns null.
     *
     * @return resolves private table name to TableToken. If no token exists, returns null
     */
    TableToken getTableTokenByPrivateTableName(String privateTableName, int tableId);

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
     * Removes table name from registry. If table name does not exist or has different table token, does nothing.
     *
     * @param tableName  name of the table to remove
     * @param tableToken table token to make sure intended table name is removed
     * @return true if table name was removed, false otherwise
     */
    boolean removeTableName(CharSequence tableName, TableToken tableToken);

    /**
     * Removes table token from registry. To be called when table is fully dropped and will exclude
     * the table token to appear in {@link #getTableTokens()}.
     *
     * @param tableToken table token to remove
     */
    void removeTableToken(TableToken tableToken);

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
