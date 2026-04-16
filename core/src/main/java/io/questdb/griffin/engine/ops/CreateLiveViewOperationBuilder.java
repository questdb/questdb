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

import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Mutable;

public class CreateLiveViewOperationBuilder implements ExecutionModel, Mutable {
    private String baseTableName;
    private boolean ignoreIfExists;
    private char lagUnit;
    private long lagValue;
    private char retentionUnit;
    private long retentionValue;
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
                lagValue,
                lagUnit,
                retentionValue,
                retentionUnit,
                ignoreIfExists
        );
    }

    @Override
    public void clear() {
        baseTableName = null;
        ignoreIfExists = false;
        lagValue = 0;
        lagUnit = 0;
        retentionValue = 0;
        retentionUnit = 0;
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

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setIgnoreIfExists(boolean ignoreIfExists) {
        this.ignoreIfExists = ignoreIfExists;
    }

    public void setLagUnit(char lagUnit) {
        this.lagUnit = lagUnit;
    }

    public void setLagValue(long lagValue) {
        this.lagValue = lagValue;
    }

    public void setRetentionUnit(char retentionUnit) {
        this.retentionUnit = retentionUnit;
    }

    public void setRetentionValue(long retentionValue) {
        this.retentionValue = retentionValue;
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
