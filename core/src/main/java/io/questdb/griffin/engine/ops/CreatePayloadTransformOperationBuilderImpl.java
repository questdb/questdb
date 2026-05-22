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

import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public class CreatePayloadTransformOperationBuilderImpl implements CreatePayloadTransformOperationBuilder, Mutable {
    private int dlqPartitionBy = -1;
    private CharSequence dlqTable;
    private int dlqTablePosition;
    private CharSequence dlqTtlUnit;
    private int dlqTtlUnitPosition;
    private long dlqTtlValue;
    private int dlqTtlValuePosition;
    private boolean ignoreIfExists;
    private boolean isReplace;
    private CharSequence name;
    private int namePosition;
    private CharSequence selectSql;
    private int selectSqlPosition;
    private CharSequence targetTable;
    private int targetTablePosition;

    @Override
    public CreatePayloadTransformOperation build(CharSequence sqlText) {
        return new CreatePayloadTransformOperation(
                Chars.toString(sqlText),
                Chars.toString(name),
                namePosition,
                Chars.toString(targetTable),
                targetTablePosition,
                Chars.toString(selectSql),
                selectSqlPosition,
                dlqTable != null ? Chars.toString(dlqTable) : null,
                dlqTablePosition,
                dlqPartitionBy,
                dlqTtlValue,
                dlqTtlValuePosition,
                dlqTtlUnit != null ? Chars.toString(dlqTtlUnit) : null,
                dlqTtlUnitPosition,
                ignoreIfExists,
                isReplace
        );
    }

    @Override
    public void clear() {
        name = null;
        namePosition = 0;
        targetTable = null;
        targetTablePosition = 0;
        selectSql = null;
        selectSqlPosition = 0;
        dlqTable = null;
        dlqTablePosition = 0;
        dlqPartitionBy = -1;
        dlqTtlValue = 0;
        dlqTtlValuePosition = 0;
        dlqTtlUnit = null;
        dlqTtlUnitPosition = 0;
        ignoreIfExists = false;
        isReplace = false;
    }

    public void setDlqPartitionBy(int dlqPartitionBy) {
        this.dlqPartitionBy = dlqPartitionBy;
    }

    public void setDlqTable(CharSequence dlqTable) {
        this.dlqTable = dlqTable;
    }

    public void setDlqTablePosition(int dlqTablePosition) {
        this.dlqTablePosition = dlqTablePosition;
    }

    public void setDlqTtlUnit(CharSequence dlqTtlUnit) {
        this.dlqTtlUnit = dlqTtlUnit;
    }

    public void setDlqTtlUnitPosition(int dlqTtlUnitPosition) {
        this.dlqTtlUnitPosition = dlqTtlUnitPosition;
    }

    public void setDlqTtlValue(long dlqTtlValue) {
        this.dlqTtlValue = dlqTtlValue;
    }

    public void setDlqTtlValuePosition(int dlqTtlValuePosition) {
        this.dlqTtlValuePosition = dlqTtlValuePosition;
    }

    public void setIgnoreIfExists(boolean ignoreIfExists) {
        this.ignoreIfExists = ignoreIfExists;
    }

    public void setName(CharSequence name) {
        this.name = name;
    }

    public void setNamePosition(int namePosition) {
        this.namePosition = namePosition;
    }

    public void setReplace(boolean isReplace) {
        this.isReplace = isReplace;
    }

    public void setSelectSql(CharSequence selectSql) {
        this.selectSql = selectSql;
    }

    public void setSelectSqlPosition(int selectSqlPosition) {
        this.selectSqlPosition = selectSqlPosition;
    }

    public void setTargetTable(CharSequence targetTable) {
        this.targetTable = targetTable;
    }

    public void setTargetTablePosition(int targetTablePosition) {
        this.targetTablePosition = targetTablePosition;
    }
}
