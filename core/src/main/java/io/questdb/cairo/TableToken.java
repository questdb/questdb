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

package io.questdb.cairo;

import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.GcUtf8String;
import org.jetbrains.annotations.NotNull;

public class TableToken implements Sinkable {
    @NotNull
    private final GcUtf8String dirName;
    private final boolean isWal;
    private final int tableId;
    @NotNull
    private final String tableName;

    public TableToken(@NotNull String tableName, @NotNull String dirName, int tableId, boolean isWal) {
        this(tableName, new GcUtf8String(dirName), tableId, isWal);
    }

    private TableToken(@NotNull String tableName, @NotNull GcUtf8String dirName, int tableId, boolean isWal) {
        this.tableName = tableName;
        this.dirName = dirName;
        this.tableId = tableId;
        this.isWal = isWal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableToken that = (TableToken) o;

        if (tableId != that.tableId) return false;
        if (isWal != that.isWal) return false;
        if (!tableName.equals(that.tableName)) return false;
        return dirName.equals(that.dirName);
    }

    /**
     * @return directory where the table is located.
     */
    public @NotNull String getDirName() {
        return dirName.toString();
    }

    /**
     * @return UTF-8 buffer naming the directory where the table is located.
     */
    public @NotNull DirectUtf8Sequence getDirNameUtf8() {
        return dirName;
    }

    /**
     * @return table id
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return table name
     */
    public @NotNull String getTableName() {
        return tableName;
    }

    @Override
    public int hashCode() {
        return tableId;
    }

    public boolean isWal() {
        return isWal;
    }

    public TableToken renamed(String newName) {
        return new TableToken(newName, dirName, tableId, isWal);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.encodeUtf8(tableName);
    }

    @Override
    public String toString() {
        return "TableToken{" +
                "tableName=" + tableName +
                ", dirName=" + dirName +
                ", tableId=" + tableId +
                ", isWal=" + isWal +
                '}';
    }
}
