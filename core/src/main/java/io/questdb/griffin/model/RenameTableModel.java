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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class RenameTableModel implements Mutable, ExecutionModel {
    public static final ObjectFactory<RenameTableModel> FACTORY = RenameTableModel::new;

    private ExpressionNode from;
    private ExpressionNode to;

    private RenameTableModel() {
    }

    @Override
    public void clear() {
        from = to = null;
    }

    public ExpressionNode getFrom() {
        return from;
    }

    @Override
    public int getModelType() {
        return ExecutionModel.RENAME_TABLE;
    }

    @Override
    public CharSequence getTableName() {
        return from.token;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return from;
    }

    public ExpressionNode getTo() {
        return to;
    }

    public void setFrom(ExpressionNode from) {
        this.from = from;
    }

    public void setTo(ExpressionNode to) {
        this.to = to;
    }
}
