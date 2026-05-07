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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

public class CreateLiveViewOperationBuilder implements ExecutionModel, Mutable {
    private @Nullable LiveViewDefinition.LvAnchorSpec anchorSpec;
    private String baseTableName;
    private long flushEveryInterval;
    private char flushEveryIntervalUnit;
    private boolean ignoreIfExists;
    private long inMemoryInterval;
    private char inMemoryIntervalUnit;
    // PartitionBy.NONE means "inherit from base"; resolved at CREATE in CairoEngine.
    private int partitionBy = PartitionBy.NONE;
    private IQueryModel selectModel;
    private String selectSql;
    private String viewName;
    private int viewNamePosition;

    public CreateLiveViewOperation build(CharSequence sqlText) {
        return new CreateLiveViewOperation(
                viewName,
                viewNamePosition,
                baseTableName,
                selectSql,
                flushEveryInterval,
                flushEveryIntervalUnit,
                inMemoryInterval,
                inMemoryIntervalUnit,
                partitionBy,
                ignoreIfExists,
                anchorSpec
        );
    }

    @Override
    public void clear() {
        anchorSpec = null;
        baseTableName = null;
        ignoreIfExists = false;
        flushEveryInterval = 0;
        flushEveryIntervalUnit = 0;
        inMemoryInterval = 0;
        inMemoryIntervalUnit = 0;
        partitionBy = PartitionBy.NONE;
        selectModel = null;
        selectSql = null;
        viewName = null;
        viewNamePosition = 0;
    }

    @Override
    public int getModelType() {
        return CREATE_LIVE_VIEW;
    }

    @Override
    public IQueryModel getQueryModel() {
        return selectModel;
    }

    public String getViewName() {
        return viewName;
    }

    public void setAnchorSpec(@Nullable LiveViewDefinition.LvAnchorSpec anchorSpec) {
        this.anchorSpec = anchorSpec;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setFlushEveryInterval(long flushEveryInterval) {
        this.flushEveryInterval = flushEveryInterval;
    }

    public void setFlushEveryIntervalUnit(char flushEveryIntervalUnit) {
        this.flushEveryIntervalUnit = flushEveryIntervalUnit;
    }

    public void setIgnoreIfExists(boolean ignoreIfExists) {
        this.ignoreIfExists = ignoreIfExists;
    }

    public void setInMemoryInterval(long inMemoryInterval) {
        this.inMemoryInterval = inMemoryInterval;
    }

    public void setInMemoryIntervalUnit(char inMemoryIntervalUnit) {
        this.inMemoryIntervalUnit = inMemoryIntervalUnit;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setSelectModel(IQueryModel selectModel) {
        this.selectModel = selectModel;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public void setViewNamePosition(int viewNamePosition) {
        this.viewNamePosition = viewNamePosition;
    }
}
