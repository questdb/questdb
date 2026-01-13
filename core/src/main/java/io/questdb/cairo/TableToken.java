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

import io.questdb.std.Chars;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Stands for a WAL table, or a non-WAL table, or a materialized view.
 */
public class TableToken implements Sinkable {
    private final String dbLogName;
    @NotNull
    private final GcUtf8String dirName;
    private final boolean dirNameSameAsTableName;
    private final boolean isProtected;
    private final boolean isPublic;
    private final boolean isSystem;
    private final boolean isWal;
    private final int tableId;
    @NotNull
    private final String tableName;
    private final Type type;

    public TableToken(@NotNull String tableName, @NotNull String dirName, @Nullable String dbLogName, int tableId, boolean isWal, boolean isSystem, boolean isProtected) {
        this(tableName, new GcUtf8String(dirName), dbLogName, tableId, false, false, isWal, isSystem, isProtected, false);
    }

    public TableToken(@NotNull String tableName, @NotNull String dirName, @Nullable String dbLogName, int tableId, boolean isView, boolean isMatView, boolean isWal, boolean isSystem, boolean isProtected, boolean isPublic) {
        this(tableName, new GcUtf8String(dirName), dbLogName, tableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);
    }

    private TableToken(@NotNull String tableName, @NotNull GcUtf8String dirName, @Nullable String dbLogName, int tableId, boolean isView, boolean isMatView, boolean isWal, boolean isSystem, boolean isProtected, boolean isPublic) {
        this.tableName = tableName;
        this.dirName = dirName;
        this.tableId = tableId;
        this.type = isMatView ? Type.MAT_VIEW : isView ? Type.VIEW : Type.TABLE;
        this.isWal = isWal;
        this.isSystem = isSystem;
        this.isProtected = isProtected;
        this.isPublic = isPublic;
        this.dbLogName = dbLogName;
        String dirNameString = dirName.toString();
        this.dirNameSameAsTableName = Chars.startsWith(dirNameString, tableName) &&
                (dirNameString.length() == tableName.length() ||
                        (dirNameString.length() > tableName.length() && dirNameString.charAt(tableName.length()) == '~'));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableToken that = (TableToken) o;

        if (tableId != that.tableId) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (isWal != that.isWal) {
            return false;
        }
        if (isSystem != that.isSystem) {
            return false;
        }
        if (isProtected != that.isProtected) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
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

    public Type getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return tableId;
    }

    public boolean isMatView() {
        return type == Type.MAT_VIEW;
    }

    public boolean isProtected() {
        return isProtected;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public boolean isSystem() {
        return isSystem;
    }

    public boolean isView() {
        return type == Type.VIEW;
    }

    public boolean isWal() {
        return isWal;
    }

    public TableToken renamed(String newName) {
        return new TableToken(newName, dirName, dbLogName, tableId, isView(), isMatView(), isWal, isSystem, isProtected, isPublic);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        if (dbLogName != null) {
            sink.put(dbLogName).put('/');
        }
        if (dirNameSameAsTableName) {
            sink.put(dirName);
        } else {
            sink.put("TableToken{tableName=").put(tableName)
                    .put(", dirName=").put(dirName)
                    .put('}');
        }
    }

    @Override
    public String toString() {
        return "TableToken{" +
                "tableName=" + tableName +
                ", dirName=" + dirName +
                ", tableId=" + tableId +
                ", isView=" + isView() +
                ", isMatView=" + isMatView() +
                ", isWal=" + isWal +
                ", isSystem=" + isSystem +
                ", isProtected=" + isProtected +
                ", isPublic=" + isPublic +
                '}';
    }

    public enum Type {
        TABLE, VIEW, MAT_VIEW
    }
}
