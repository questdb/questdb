/*+*****************************************************************************
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

package io.questdb.griffin.plan.logical;

import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import java.util.Objects;

/**
 * Compilation-owned column descriptions aligned with stable logical IDs.
 * Names, qualifiers and nested schemas are borrowed immutable descriptions for this compilation;
 * copying or clearing a schema does not mutate them. Executable factories must export
 * independently owned metadata and names before compilation storage is reused.
 */
public final class OutputSchema implements Mutable {
    public static final int COLUMN_AMBIGUOUS = -2;
    private static final int VISIBLE = 1;
    private static final int SYMBOL_TABLE_STATIC = 2;
    private static final int NAME_PROTECTED = 4;
    private final IntList columnIds = new IntList();
    private final ObjList<OutputSchema> columnMetadata = new ObjList<>();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final ObjList<CharSequence> columnQualifiers = new ObjList<>();
    private final IntList columnTypes = new IntList();
    private final IntList columnFlags = new IntList();
    private int timestampIndex = -1;

    public OutputSchema add(int columnId, CharSequence name, int type, boolean isVisible) {
        return add(columnId, name, type, null, isVisible);
    }

    public OutputSchema add(int columnId, CharSequence name, int type, OutputSchema metadata, boolean isVisible) {
        return add(columnId, name, type, metadata, isVisible, null);
    }

    public OutputSchema add(int columnId, CharSequence name, int type, OutputSchema metadata, boolean isVisible, CharSequence qualifier) {
        if (columnId < 0) {
            throw new IllegalArgumentException("negative logical column ID");
        }
        Objects.requireNonNull(name, "name");
        final int count = getColumnCount();
        columnIds.checkCapacity(count + 1);
        columnMetadata.checkCapacity(count + 1);
        columnNames.checkCapacity(count + 1);
        columnQualifiers.checkCapacity(count + 1);
        columnTypes.checkCapacity(count + 1);
        columnFlags.checkCapacity(count + 1);
        columnIds.add(columnId);
        columnMetadata.add(metadata);
        columnNames.add(name);
        columnQualifiers.add(qualifier);
        columnTypes.add(type);
        columnFlags.add(isVisible ? VISIBLE : 0);
        return this;
    }

    @Override
    public void clear() {
        columnIds.clear();
        columnMetadata.clear();
        columnNames.clear();
        columnQualifiers.clear();
        columnTypes.clear();
        columnFlags.clear();
        timestampIndex = -1;
    }

    public void copyFrom(OutputSchema that) {
        if (that != this) {
            clear();
            columnIds.addAll(that.columnIds);
            columnMetadata.addAll(that.columnMetadata);
            columnNames.addAll(that.columnNames);
            columnQualifiers.addAll(that.columnQualifiers);
            columnTypes.addAll(that.columnTypes);
            columnFlags.addAll(that.columnFlags);
            timestampIndex = that.timestampIndex;
        }
    }

    public int getColumnCount() {
        return columnIds.size();
    }

    public int getColumnId(int index) {
        return columnIds.getQuick(index);
    }

    public int getColumnIndexById(int columnId) {
        return columnIds.indexOf(columnId, 0, columnIds.size());
    }

    public int getColumnIndexQuiet(CharSequence name) {
        return getColumnIndexQuiet(name, 0, name.length());
    }

    public int getColumnIndexQuiet(CharSequence name, int lo, int hi) {
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence column = columnNames.getQuick(i);
            if (isReferenceable(i) && column.length() == hi - lo && Chars.equalsIgnoreCase(column, name, lo, hi)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Resolves a visible source reference, returning -1 when absent or {@link #COLUMN_AMBIGUOUS}
     * when several columns match. A null qualifier searches the whole current scope.
     * The ordinary name lookup above intentionally retains projection-alias precedence.
     */
    public int getColumnIndexQuiet(CharSequence qualifier, CharSequence name, int lo, int hi) {
        int index = -1;
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence column = columnNames.getQuick(i);
            if (isReferenceable(i) && (qualifier == null || Chars.equalsIgnoreCaseNc(qualifier, columnQualifiers.getQuick(i)))
                    && column.length() == hi - lo && Chars.equalsIgnoreCase(column, name, lo, hi)) {
                if (index != -1) {
                    return COLUMN_AMBIGUOUS;
                }
                index = i;
            }
        }
        return index;
    }

    public CharSequence getColumnName(int index) {
        return columnNames.getQuick(index);
    }

    public CharSequence getColumnQualifier(int index) {
        return columnQualifiers.getQuick(index);
    }

    public int getColumnType(int index) {
        return columnTypes.getQuick(index);
    }

    public void remove(int index) {
        columnIds.removeIndex(index);
        columnMetadata.remove(index);
        columnNames.remove(index);
        columnQualifiers.remove(index);
        columnTypes.removeIndex(index);
        columnFlags.removeIndex(index);
        if (timestampIndex == index) {
            timestampIndex = -1;
        } else if (timestampIndex > index) {
            timestampIndex--;
        }
    }

    public void setColumnId(int index, int columnId) {
        if (columnId < 0) {
            throw new IllegalArgumentException("negative logical column ID");
        }
        columnIds.setQuick(index, columnId);
    }

    public void setColumnName(int index, CharSequence name, CharSequence qualifier) {
        columnNames.setQuick(index, name);
        columnQualifiers.setQuick(index, qualifier);
    }

    public void setColumnType(int index, int type) {
        columnTypes.setQuick(index, type);
    }

    public OutputSchema getMetadata(int index) {
        return columnMetadata.getQuick(index);
    }

    public int getTimestampColumnId() {
        return timestampIndex < 0 ? -1 : getColumnId(timestampIndex);
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public boolean hasColumnQualifier(CharSequence qualifier) {
        for (int i = 0, n = columnQualifiers.size(); i < n; i++) {
            if (Chars.equalsIgnoreCaseNc(qualifier, columnQualifiers.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    public boolean hasColumnQualifiers() {
        for (int i = 0, n = columnQualifiers.size(); i < n; i++) {
            if (columnQualifiers.getQuick(i) != null) {
                return true;
            }
        }
        return false;
    }

    public boolean isNameProtected(int index) {
        return (columnFlags.getQuick(index) & NAME_PROTECTED) != 0;
    }

    /**
     * A compiler-protected name (dotted, or an operator token) that enclosing queries cannot reference.
     */
    public void protectName(int index) {
        columnFlags.setQuick(index, columnFlags.getQuick(index) | NAME_PROTECTED);
    }

    public boolean isSymbolTableStatic(int index) {
        return (columnFlags.getQuick(index) & SYMBOL_TABLE_STATIC) != 0;
    }

    public void setSymbolTableStatic(int index, boolean isStatic) {
        final int flags = columnFlags.getQuick(index);
        columnFlags.setQuick(index, isStatic ? flags | SYMBOL_TABLE_STATIC : flags & ~SYMBOL_TABLE_STATIC);
    }

    public boolean isVisible(int index) {
        return (columnFlags.getQuick(index) & VISIBLE) != 0;
    }

    private boolean isReferenceable(int index) {
        return (columnFlags.getQuick(index) & (VISIBLE | NAME_PROTECTED)) == VISIBLE;
    }

    public void setTimestampIndex(int index) {
        if (index < -1 || index >= getColumnCount()) {
            throw new IndexOutOfBoundsException("timestamp column index: " + index);
        }
        timestampIndex = index;
    }
}
