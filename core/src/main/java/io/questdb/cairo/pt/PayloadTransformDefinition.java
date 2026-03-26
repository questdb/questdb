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

package io.questdb.cairo.pt;

import io.questdb.std.Mutable;

public class PayloadTransformDefinition implements Mutable {
    private int dlqPartitionBy;
    private String dlqTable;
    private String dlqTtlUnit;
    private long dlqTtlValue;
    private String name;
    private String selectSql;
    private String targetTable;

    @Override
    public void clear() {
        name = null;
        targetTable = null;
        selectSql = null;
        dlqTable = null;
        dlqPartitionBy = -1;
        dlqTtlValue = 0;
        dlqTtlUnit = null;
    }

    public void copyFrom(PayloadTransformDefinition other) {
        this.name = other.name;
        this.targetTable = other.targetTable;
        this.selectSql = other.selectSql;
        this.dlqTable = other.dlqTable;
        this.dlqPartitionBy = other.dlqPartitionBy;
        this.dlqTtlValue = other.dlqTtlValue;
        this.dlqTtlUnit = other.dlqTtlUnit;
    }

    public int getDlqPartitionBy() {
        return dlqPartitionBy;
    }

    public String getDlqTable() {
        return dlqTable;
    }

    public String getDlqTtlUnit() {
        return dlqTtlUnit;
    }

    public long getDlqTtlValue() {
        return dlqTtlValue;
    }

    public String getName() {
        return name;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setDlqPartitionBy(int dlqPartitionBy) {
        this.dlqPartitionBy = dlqPartitionBy;
    }

    public void setDlqTable(String dlqTable) {
        this.dlqTable = dlqTable;
    }

    public void setDlqTtlUnit(String dlqTtlUnit) {
        this.dlqTtlUnit = dlqTtlUnit;
    }

    public void setDlqTtlValue(long dlqTtlValue) {
        this.dlqTtlValue = dlqTtlValue;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }
}
