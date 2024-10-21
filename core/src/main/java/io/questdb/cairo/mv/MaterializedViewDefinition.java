/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private long sampleByPeriodMicros;
    private CharSequence parentTableName;
    private long sampleByFromEpochMicros;
    private CharSequence viewSql;
    private TableToken tableToken;

    public CharSequence getBaseTableName() {
        return parentTableName;
    }

    public long getSampleByFromEpochMicros() {
        return sampleByFromEpochMicros;
    }

    public long getSampleByPeriodMicros() {
        return sampleByPeriodMicros;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public CharSequence getViewSql() {
        return viewSql;
    }

    public void setParentTableName(CharSequence parentTableName) {
        this.parentTableName = parentTableName;
    }

    public void setSampleByFromEpochMicros(long sampleByFromEpochMicros) {
        this.sampleByFromEpochMicros = sampleByFromEpochMicros;
    }

    public void setSampleByPeriodMicros(long sampleByPeriodMicros) {
        this.sampleByPeriodMicros = sampleByPeriodMicros;
    }

    public void setTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    public void setViewSql(CharSequence viewSql) {
        this.viewSql = viewSql;
    }
}
