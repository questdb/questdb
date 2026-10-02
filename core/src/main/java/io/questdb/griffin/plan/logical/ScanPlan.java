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
import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class ScanPlan extends LogicalPlan {
    public static final ObjectFactory<ScanPlan> FACTORY = ScanPlan::new;
    public static final int HINT_FORCE_USE_COVERING = 4;
    public static final int HINT_NO_COVERING = 2;
    public static final int HINT_NO_INDEX = 1;
    public static final int HINT_NO_SYMBOL_PATTERN_INDEX = 8;
    public static final int HINT_PRE_TOUCH = 16;
    private final IntList indexedColumnIds = new IntList();
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
        indexedColumnIds.clear();
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

    @Override
    public Type getType() {
        return Type.SCAN;
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

    public boolean isUpdate() {
        return isUpdate;
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
}
