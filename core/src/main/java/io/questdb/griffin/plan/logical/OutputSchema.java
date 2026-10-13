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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
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
    private static final int ID_INDEX_MIN_COLUMNS = 64;
    private static final int INDEX_MIN_SCANS = 8;
    private static final int NAME_INDEX_MIN_COLUMNS = 16;
    private final IntList columnIds = new IntList();
    private final ObjList<OutputSchema> columnMetadata = new ObjList<>();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final ObjList<CharSequence> columnQualifiers = new ObjList<>();
    private final IntList columnTypes = new IntList();
    private final IntList columnFlags = new IntList();
    private final IntIntHashMap idIndexes = new IntIntHashMap();
    private final IntList nameSlots = new IntList();
    private final IntList sameNamePrevious = new IntList();
    private int idIndexedCount;
    private int idScanCount;
    private int nameIndexedCount;
    private int nameScanCount;
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

    /**
     * Appends column {@code index} of {@code source} under {@code columnId}, {@code name} and {@code isVisible},
     * with the source's type, nested schema, symbol-table capability and name protection, and no qualifier.
     */
    public void addColumnAs(OutputSchema source, int index, int columnId, CharSequence name, boolean isVisible) {
        addColumnAs(source, index, columnId, name, isVisible, null);
    }

    /**
     * Appends column {@code index} of {@code source} under {@code columnId}, {@code name}, {@code isVisible} and
     * {@code qualifier}, with the source's type, nested schema, symbol-table capability and name protection.
     */
    public void addColumnAs(OutputSchema source, int index, int columnId, CharSequence name, boolean isVisible, CharSequence qualifier) {
        add(columnId, name, source.getColumnType(index), source.getMetadata(index), isVisible, qualifier);
        columnFlags.setQuick(getColumnCount() - 1, source.columnFlags.getQuick(index) & ~VISIBLE | (isVisible ? VISIBLE : 0));
    }

    /**
     * Appends column {@code index} of {@code source} with every attribute: id, name, qualifier, type, nested
     * schema, visibility, symbol-table capability and name protection. The designated timestamp is not touched.
     */
    public void addColumnFrom(OutputSchema source, int index) {
        add(source.getColumnId(index), source.getColumnName(index), source.getColumnType(index), source.getMetadata(index),
                source.isVisible(index), source.getColumnQualifier(index));
        columnFlags.setQuick(getColumnCount() - 1, source.columnFlags.getQuick(index));
    }

    /**
     * Appends column {@code index} of {@code metadata} under {@code columnId}, visible, with its name, type and
     * symbol-table capability.
     */
    public void addColumnFrom(RecordMetadata metadata, int index, int columnId) {
        add(columnId, metadata.getColumnName(index), metadata.getColumnType(index), true);
        setSymbolTableStatic(getColumnCount() - 1, metadata.isSymbolTableStatic(index));
    }

    /**
     * Appends every column of {@code source} as {@link #addColumnFrom} does.
     */
    public void addColumnsFrom(OutputSchema source) {
        for (int i = 0, n = source.getColumnCount(); i < n; i++) {
            addColumnFrom(source, i);
        }
    }

    /**
     * Appends every column of {@code source} to a join scope: under {@code qualifier}, or under its own qualifier
     * when {@code qualifier} is null, and without name protection, which guards a projection's own alias only; a
     * join scope names its inputs' columns bare, so that the composed reference resolves. The other attributes are
     * kept as {@link #addColumnFrom} keeps them.
     */
    public void addColumnsFrom(OutputSchema source, CharSequence qualifier) {
        for (int i = 0, n = source.getColumnCount(); i < n; i++) {
            addColumnFrom(source, i);
            final int index = getColumnCount() - 1;
            columnFlags.setQuick(index, columnFlags.getQuick(index) & ~NAME_PROTECTED);
            if (qualifier != null) {
                columnQualifiers.setQuick(index, qualifier);
            }
        }
    }

    /**
     * Appends, hidden, every column of {@code source} the schema lacks, with its name, type, metadata and qualifier.
     */
    public void addMissingColumnsFrom(OutputSchema source) {
        for (int i = 0, n = source.getColumnCount(); i < n; i++) {
            if (getColumnIndexById(source.getColumnId(i)) < 0) {
                add(source.getColumnId(i), source.getColumnName(i), source.getColumnType(i), source.getMetadata(i), false,
                        source.getColumnQualifier(i));
            }
        }
    }

    @Override
    public void clear() {
        columnIds.clear();
        truncate(columnMetadata, 0);
        truncate(columnNames, 0);
        truncate(columnQualifiers, 0);
        columnTypes.clear();
        columnFlags.clear();
        idIndexedCount = 0;
        idScanCount = 0;
        nameIndexedCount = 0;
        nameScanCount = 0;
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
        if (!isIdIndexWorthwhile()) {
            return columnIds.indexOf(columnId, 0, columnIds.size());
        }
        indexIds();
        final int keyIndex = idIndexes.keyIndex(columnId);
        return keyIndex < 0 ? idIndexes.valueAt(keyIndex) : -1;
    }

    public int getColumnIndexQuiet(CharSequence name) {
        return getColumnIndexQuiet(name, 0, name.length());
    }

    public int getColumnIndexQuiet(CharSequence name, int lo, int hi) {
        if (isNameIndexWorthwhile()) {
            int index = findIndexedColumn(name, lo, hi);
            if (index > -1) {
                for (int previous = sameNamePrevious.getQuick(index); previous > -1; previous = sameNamePrevious.getQuick(previous)) {
                    index = previous;
                }
            }
            return index;
        }
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
        if (isNameIndexWorthwhile()) {
            for (int i = findIndexedColumn(name, lo, hi); i > -1; i = sameNamePrevious.getQuick(i)) {
                if (qualifier == null || Chars.equalsIgnoreCaseNc(qualifier, columnQualifiers.getQuick(i))) {
                    if (index != -1) {
                        return COLUMN_AMBIGUOUS;
                    }
                    index = i;
                }
            }
            return index;
        }
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

    public OutputSchema getMetadata(int index) {
        return columnMetadata.getQuick(index);
    }

    public int getTimestampColumnId() {
        return timestampIndex < 0 ? -1 : getColumnId(timestampIndex);
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    /**
     * Whether a column, visible or not, has the name in any case.
     */
    public boolean hasColumnName(CharSequence name) {
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(columnNames.getQuick(i), name)) {
                return true;
            }
        }
        return false;
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

    /**
     * Whether {@code metadata} has the schema's column count and types, in order.
     */
    public boolean hasColumnTypesOf(RecordMetadata metadata) {
        if (metadata.getColumnCount() != getColumnCount()) {
            return false;
        }
        for (int i = 0, n = getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) != getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean isNameProtected(int index) {
        return (columnFlags.getQuick(index) & NAME_PROTECTED) != 0;
    }

    public boolean isSymbolTableStatic(int index) {
        return (columnFlags.getQuick(index) & SYMBOL_TABLE_STATIC) != 0;
    }

    public boolean isVisible(int index) {
        return (columnFlags.getQuick(index) & VISIBLE) != 0;
    }

    /**
     * A compiler-protected name (dotted, or an operator token) that enclosing queries cannot reference.
     */
    public void protectName(int index) {
        invalidateNameIndex(index);
        columnFlags.setQuick(index, columnFlags.getQuick(index) | NAME_PROTECTED);
    }

    public void remove(int index) {
        invalidateIdIndex(index);
        invalidateNameIndex(index);
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

    /**
     * Keeps the columns at {@code indexes}, in that order, each with every attribute; the designated timestamp
     * stays designated when it is kept, else the schema designates none.
     */
    public void retain(IntList indexes) {
        final int count = getColumnCount();
        final int timestampId = getTimestampColumnId();
        final int n = indexes.size();
        for (int i = 0; i < n; i++) {
            addColumnFrom(this, indexes.getQuick(i));
        }
        for (int i = 0; i < n; i++) {
            columnIds.setQuick(i, columnIds.getQuick(count + i));
            columnMetadata.setQuick(i, columnMetadata.getQuick(count + i));
            columnNames.setQuick(i, columnNames.getQuick(count + i));
            columnQualifiers.setQuick(i, columnQualifiers.getQuick(count + i));
            columnTypes.setQuick(i, columnTypes.getQuick(count + i));
            columnFlags.setQuick(i, columnFlags.getQuick(count + i));
        }
        columnIds.setPos(n);
        truncate(columnMetadata, n);
        truncate(columnNames, n);
        truncate(columnQualifiers, n);
        columnTypes.setPos(n);
        columnFlags.setPos(n);
        idIndexedCount = 0;
        nameIndexedCount = 0;
        setTimestampColumnId(timestampId);
    }

    public void setColumnId(int index, int columnId) {
        if (columnId < 0) {
            throw new IllegalArgumentException("negative logical column ID");
        }
        invalidateIdIndex(index);
        columnIds.setQuick(index, columnId);
    }

    public void setColumnName(int index, CharSequence name, CharSequence qualifier) {
        invalidateNameIndex(index);
        columnNames.setQuick(index, name);
        columnQualifiers.setQuick(index, qualifier);
    }

    public void setColumnType(int index, int type) {
        columnTypes.setQuick(index, type);
    }

    public void setSymbolTableStatic(int index, boolean isStatic) {
        final int flags = columnFlags.getQuick(index);
        columnFlags.setQuick(index, isStatic ? flags | SYMBOL_TABLE_STATIC : flags & ~SYMBOL_TABLE_STATIC);
    }

    /**
     * Designates the column with the id, or none when the schema does not list it.
     */
    public void setTimestampColumnId(int columnId) {
        timestampIndex = getColumnIndexById(columnId);
    }

    public void setTimestampIndex(int index) {
        if (index < -1 || index >= getColumnCount()) {
            throw new IndexOutOfBoundsException("timestamp column index: " + index);
        }
        timestampIndex = index;
    }

    private static void truncate(ObjList<?> list, int size) {
        list.set(size, list.size(), null);
        list.setPos(size);
    }

    /**
     * Returns the last referenceable column named {@code name[lo, hi)} in any case, or -1; earlier ones chain
     * through {@link #sameNamePrevious}.
     */
    private int findIndexedColumn(CharSequence name, int lo, int hi) {
        indexNames();
        final int mask = nameSlots.size() - 1;
        for (int slot = Chars.lowerCaseHashCode(name, lo, hi) & mask; ; slot = (slot + 1) & mask) {
            final int index = nameSlots.getQuick(slot) - 1;
            if (index < 0) {
                return -1;
            }
            final CharSequence column = columnNames.getQuick(index);
            if (column.length() == hi - lo && Chars.equalsIgnoreCase(column, name, lo, hi)) {
                return index;
            }
        }
    }

    /**
     * Extends the id index over the columns appended since the last lookup, rebuilding it after an invalidation;
     * an id keeps its first index.
     */
    private void indexIds() {
        final int count = columnIds.size();
        if (idIndexedCount == 0) {
            idIndexes.clear();
        }
        for (int i = idIndexedCount; i < count; i++) {
            final int keyIndex = idIndexes.keyIndex(columnIds.getQuick(i));
            if (keyIndex >= 0) {
                idIndexes.putAt(keyIndex, columnIds.getQuick(i), i);
            }
        }
        idIndexedCount = count;
    }

    /**
     * Extends the name index over the columns appended since the last lookup, rebuilding it after an
     * invalidation or when it must grow.
     */
    private void indexNames() {
        final int count = columnNames.size();
        if (nameIndexedCount == count) {
            return;
        }
        final int capacity = Numbers.ceilPow2(count * 2);
        if (nameIndexedCount == 0 || nameSlots.size() < capacity) {
            nameIndexedCount = 0;
            nameSlots.setAll(Math.max(capacity, nameSlots.size()), 0);
        }
        sameNamePrevious.setPos(count);
        final int mask = nameSlots.size() - 1;
        for (int i = nameIndexedCount; i < count; i++) {
            if (isReferenceable(i)) {
                final CharSequence name = columnNames.getQuick(i);
                int slot = Chars.lowerCaseHashCode(name) & mask;
                int indexed;
                while ((indexed = nameSlots.getQuick(slot)) != 0 && !Chars.equalsIgnoreCase(columnNames.getQuick(indexed - 1), name)) {
                    slot = (slot + 1) & mask;
                }
                sameNamePrevious.setQuick(i, indexed - 1);
                nameSlots.setQuick(slot, i + 1);
            }
        }
        nameIndexedCount = count;
    }

    private void invalidateIdIndex(int index) {
        if (index < idIndexedCount) {
            idIndexedCount = 0;
        }
    }

    private void invalidateNameIndex(int index) {
        if (index < nameIndexedCount) {
            nameIndexedCount = 0;
        }
    }

    /**
     * Whether a lookup should use the id index: the schema is wide, and the index is built or the schema has
     * already been scanned often enough to amortise building it.
     */
    private boolean isIdIndexWorthwhile() {
        return columnIds.size() >= ID_INDEX_MIN_COLUMNS && (idIndexedCount > 0 || ++idScanCount > INDEX_MIN_SCANS);
    }

    /**
     * Whether a lookup should use the name index: the schema is wide, and the index is built or the schema has
     * already been scanned often enough to amortise building it.
     */
    private boolean isNameIndexWorthwhile() {
        return columnNames.size() >= NAME_INDEX_MIN_COLUMNS && (nameIndexedCount > 0 || ++nameScanCount > INDEX_MIN_SCANS);
    }

    private boolean isReferenceable(int index) {
        return (columnFlags.getQuick(index) & (VISIBLE | NAME_PROTECTED)) == VISIBLE;
    }

}
