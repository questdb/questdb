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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class ScanPlan extends LogicalPlan {
    public static final ObjectFactory<ScanPlan> FACTORY = ScanPlan::new;
    public static final int HINT_FORCE_USE_COVERING = 4;
    public static final int HINT_NO_COVERING = 2;
    public static final int HINT_NO_INDEX = 1;
    public static final int HINT_NO_SYMBOL_PATTERN_INDEX = 8;
    public static final int HINT_PRE_TOUCH = 16;
    private final IntList authorizedColumnIndexes = new IntList();
    private final IntList authorizedColumns = new IntList();
    private final IntList indexedColumnIds = new IntList();
    private final IntList referencedColumnIndexes = new IntList();
    private final ObjList<CharSequence> referencedColumnNames = new ObjList<>();
    private final IntList referencedColumnPositions = new IntList();
    private final IntList sourceColumnIndexes = new IntList();
    private int hints;
    private boolean isRandomAccess = true;
    private boolean isUpdate;
    private long metadataVersion = -1;
    private int nativeTimestampColumnId = -1;
    private int nativeTimestampType = ColumnType.UNDEFINED;
    private TableToken tableToken;
    private String viewName;
    private int viewPosition = -1;

    @Override
    public void clear() {
        super.clear();
        authorizedColumnIndexes.clear();
        authorizedColumns.clear();
        indexedColumnIds.clear();
        referencedColumnIndexes.clear();
        referencedColumnNames.clear();
        referencedColumnPositions.clear();
        sourceColumnIndexes.clear();
        metadataVersion = -1;
        nativeTimestampColumnId = -1;
        nativeTimestampType = ColumnType.UNDEFINED;
        tableToken = null;
        viewName = null;
        viewPosition = -1;
        isUpdate = false;
        isRandomAccess = true;
        hints = 0;
    }

    /**
     * Adds to {@code sink} the names of the table columns the query references or the scan reads, which selecting from
     * the table requires the permission on: the referenced ones in the order the query text first references them,
     * then the ones only the scan reads, in table order.
     */
    public void collectAuthorizedColumnNames(ObjList<CharSequence> sink) {
        for (int i = 0, n = orderAuthorizedColumns(); i < n; i += 3) {
            final int origin = authorizedColumns.getQuick(i + 2);
            sink.add(origin < 0 ? getOutput().getColumnName(-origin - 1) : referencedColumnNames.getQuick(origin));
        }
    }

    /**
     * The table indexes of the columns {@link #collectAuthorizedColumnNames} names, in its order.
     */
    public IntList getAuthorizedColumnIndexes() {
        authorizedColumnIndexes.clear();
        for (int i = 0, n = orderAuthorizedColumns(); i < n; i += 3) {
            authorizedColumnIndexes.add(authorizedColumns.getQuick(i + 1));
        }
        return authorizedColumnIndexes;
    }

    public int getHints() {
        return hints;
    }

    public IntList getIndexedColumnIds() {
        return indexedColumnIds;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public int getNativeTimestampColumnId() {
        return nativeTimestampColumnId;
    }

    public int getNativeTimestampType() {
        return nativeTimestampType;
    }

    public IntList getReferencedColumnIndexes() {
        return referencedColumnIndexes;
    }

    public ObjList<CharSequence> getReferencedColumnNames() {
        return referencedColumnNames;
    }

    public IntList getReferencedColumnPositions() {
        return referencedColumnPositions;
    }

    public IntList getSourceColumnIndexes() {
        return sourceColumnIndexes;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public String getViewName() {
        return viewName;
    }

    public int getViewPosition() {
        return viewPosition;
    }

    public boolean hasHint(int hint) {
        return (hints & hint) != 0;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        throw new IndexOutOfBoundsException("scan has no input: " + index);
    }

    @Override
    public int inputCount() {
        return 0;
    }

    public boolean isRandomAccess() {
        return isRandomAccess;
    }

    public void setRandomAccess(boolean isRandomAccess) {
        this.isRandomAccess = isRandomAccess;
    }

    /**
     * True when the scan reads the target table of an UPDATE, which the binder opened for write.
     */
    public boolean isUpdate() {
        return isUpdate;
    }

    /**
     * Records the columns of the scan the query references, with the text position of their first reference; the
     * binder calls it once the query is bound, before any pass prunes them. A scan that reads a table through a view
     * records none: the view's own text references its columns, and the view's permission covers them.
     */
    public void markReferencedColumns(IntIntHashMap referencePositions) {
        final OutputSchema output = getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int index = referencePositions.keyIndex(output.getColumnId(i));
            if (index < 0) {
                referencedColumnIndexes.add(sourceColumnIndexes.getQuick(i));
                referencedColumnNames.add(output.getColumnName(i));
                referencedColumnPositions.add(referencePositions.valueAt(index));
            }
        }
    }

    public ScanPlan of(TableToken tableToken, long metadataVersion, int position) {
        return of(tableToken, metadataVersion, position, false);
    }

    public ScanPlan of(TableToken tableToken, long metadataVersion, int position, boolean isUpdate) {
        this.tableToken = Objects.requireNonNull(tableToken);
        this.metadataVersion = metadataVersion;
        this.isUpdate = isUpdate;
        setPosition(position);
        return this;
    }

    public void setHints(int hints) {
        this.hints = hints;
    }

    public void setNativeTimestamp(int columnId, int columnType) {
        nativeTimestampColumnId = columnId;
        nativeTimestampType = columnType;
    }

    public void setView(String viewName, int viewPosition) {
        this.viewName = viewName;
        this.viewPosition = viewPosition;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        throw new IndexOutOfBoundsException("scan has no input: " + index);
    }

    /**
     * Fills {@link #authorizedColumns} with one (first reference position, table index, origin) triple per column
     * the scan authorizes, ordered by position then table index, and returns its size. An origin indexes the
     * referenced columns, or is {@code -1 - i} for output column {@code i}.
     */
    private int orderAuthorizedColumns() {
        authorizedColumns.clear();
        for (int i = 0, n = referencedColumnIndexes.size(); i < n; i++) {
            authorizedColumns.add(referencedColumnIndexes.getQuick(i));
            authorizedColumns.add(referencedColumnPositions.getQuick(i));
            authorizedColumns.add(i);
        }
        for (int i = 0, n = sourceColumnIndexes.size(); i < n; i++) {
            authorizedColumns.add(sourceColumnIndexes.getQuick(i));
            authorizedColumns.add(Integer.MAX_VALUE);
            authorizedColumns.add(-i - 1);
        }
        authorizedColumns.sortGroups(3);
        int size = 0;
        for (int i = 0, n = authorizedColumns.size(); i < n; i += 3) {
            final int columnIndex = authorizedColumns.getQuick(i);
            if (size == 0 || authorizedColumns.getQuick(size - 2) != columnIndex) {
                final int position = authorizedColumns.getQuick(i + 1);
                final int origin = authorizedColumns.getQuick(i + 2);
                authorizedColumns.setQuick(size, position);
                authorizedColumns.setQuick(size + 1, columnIndex);
                authorizedColumns.setQuick(size + 2, origin);
                size += 3;
            }
        }
        authorizedColumns.setPos(size);
        authorizedColumns.sortGroups(3);
        return size;
    }
}
