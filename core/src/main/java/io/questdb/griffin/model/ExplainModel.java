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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

/**
 * Execution model for EXPLAIN statement.
 */
public class ExplainModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<ExplainModel> FACTORY = ExplainModel::new;
    public static final int FORMAT_JSON = 2;
    public static final int FORMAT_TEXT = 1;
    private int format;
    private ExecutionModel model;

    private ExplainModel() {
    }

    @Override
    public void clear() {
        format = 0;
        model = null;
    }

    public int getFormat() {
        return format;
    }

    public ExecutionModel getInnerExecutionModel() {
        return model;
    }

    @Override
    public int getModelType() {
        return EXPLAIN;
    }

    @Override
    public CharSequence getTableName() {
        return model.getTableName();
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return model.getTableNameExpr();
    }

    public void setFormat(int format) {
        this.format = format;
    }

    /**
     * Sets the execution model to be explained and marks it as an EXPLAIN query.
     * <p>
     * This method wraps the provided execution model, and if it contains a query model,
     * automatically marks it as an EXPLAIN query via {@link QueryModel#setExplainQuery(boolean)}.
     * This ensures that the query execution uses appropriate metadata sources for query analysis.
     *
     * @param model the execution model to be explained (e.g., SELECT, UPDATE, INSERT)
     * @see QueryModel#setExplainQuery(boolean)
     */
    public void setModel(ExecutionModel model) {
        this.model = model;

        final QueryModel queryModel = model.getQueryModel();
        if (queryModel != null) {
            queryModel.setExplainQuery(true);
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("EXPLAIN");
        sink.putAscii(" (FORMAT ").putAscii(format == FORMAT_TEXT ? "TEXT" : "JSON").putAscii(") ");
    }
}
