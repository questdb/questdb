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

package io.questdb.griffin;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Collects the bare select-list names of a materialized view's own SQL text from its parsed query: the select-list
 * columns that are an unqualified column reference. The optimiser reads such a name as a call to the zero-argument
 * function of the same name when no column of that name exists, so a base column added, dropped or renamed under
 * the name switches the reading, see {@link SqlCompiler#collectMatViewBareSelectNames}. The walk covers the select
 * models of the text: nested, join and union models and the sub-queries of expressions. A model expanded from a
 * regular view is not part of the text, and neither is anything below it.
 */
final class MatViewBareSelectNames {

    private MatViewBareSelectNames() {
    }

    static void collect(@Nullable IQueryModel model, LowerCaseCharSequenceHashSet sink) {
        for (IQueryModel m = model; m != null; m = m.getNestedModel()) {
            if (m.getViewNameExpr() != null) {
                // this model and the ones below it are a regular view's text
                return;
            }
            final ObjList<QueryColumn> columns = m.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                final ExpressionNode ast = columns.getQuick(i).getAst();
                if (ast.type == ExpressionNode.LITERAL && !ast.isWildcard() && Chars.indexOfLastUnquoted(ast.token, '.') < 0) {
                    sink.add(Chars.toString(ast.token));
                }
            }
            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel != m) {
                    collect(joinModel, sink);
                }
            }
            collect(m.getUnionModel(), sink);
            final ObjList<ExpressionNode> expressionModels = m.getExpressionModels();
            for (int i = 0, n = expressionModels.size(); i < n; i++) {
                collect(expressionModels.getQuick(i).queryModel, sink);
            }
        }
    }
}
