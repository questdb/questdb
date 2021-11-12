/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import static io.questdb.griffin.model.QueryModel.*;

public class UpdateModel implements Mutable, ExecutionModel, QueryWithClauseModel, Sinkable {
    private ExpressionNode updateTableAlias;
    private final ObjList<CharSequence> updatedColumns = new ObjList<>();
    private final ObjList<ExpressionNode> updateColumnExpressions = new ObjList<>();
    private QueryModel fromModel;
    private int position;
    private CharSequence updateTableName;

    @Override
    public void addWithClause(CharSequence token, WithClauseModel wcm) {

    }

    public QueryModel getQueryModel() {
        return fromModel;
    }

    @Override
    public WithClauseModel getWithClause(CharSequence token) {
        return null;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return null;
    }

    public void addWithClauses(LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses) {

    }

    @Override
    public void clear() {
        updatedColumns.clear();
        updateColumnExpressions.clear();
        updateTableName = null;
        updateTableAlias = null;
    }

    @Override
    public int getModelType() {
        return UPDATE;
    }

    public void setUpdateTableAlias(ExpressionNode updateTableAlias) {
        this.updateTableAlias = updateTableAlias;
    }

    public ExpressionNode getUpdateTableAlias() {
        return updateTableAlias;
    }

    public void setModelPosition(int position) {
        this.position = position;
    }

    public void setFromModel(QueryModel nestedModel) {
        for (int i = 0, n = updatedColumns.size(); i < n; i++) {
            nestedModel.setSelectModelType(QueryModel.SELECT_MODEL_CURSOR);
        }
        this.fromModel = nestedModel;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("update ").put(this.updateTableName);
        if (this.updateTableAlias != null) {
            sink.put(" as ").put(this.updateTableAlias);
        }
        sink.put(" set ");
        for (int i = 0, n = updatedColumns.size(); i < n; i++) {
            sink.put(updatedColumns.get(i)).put(" = ");
            sink.put(updateColumnExpressions.get(i));
            if (i < n - 1) {
                sink.put(',');
            }
        }
        if (fromModel != null && fromModel.getNestedModel() != null) {
            sink.put(" from (");
            nestedToSink0(sink, fromModel.getNestedModel());
            sink.put(")");
        }

        if (fromModel.getWhereClause() != null) {
            sink.put(" where ");
            sink.put(fromModel.getWhereClause());
        }
    }


    private static void nestedToSink0(CharSink sink, QueryModel model) {
        if (model.getTableName() != null) {
            model.getTableName().toSink(sink);
        } else {
            sink.put('(');
            nestedToSink0(sink, model.getNestedModel());
            sink.put(')');
        }
        if (model.getAlias() != null) {
            aliasToSink(model.getAlias().token, sink);
        }

        if (model.getTimestamp() != null) {
            sink.put(" timestamp (");
            model.getTimestamp().toSink(sink);
            sink.put(')');
        }

        if (model.getLatestBy().size() > 0) {
            sink.put(" latest by ");
            for (int i = 0, n = model.getLatestBy().size(); i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                model.getLatestBy().getQuick(i).toSink(sink);
            }
        }

        if (model.getOrderedJoinModels().size() > 1) {
            for (int i = 0, n = model.getOrderedJoinModels().size(); i < n; i++) {
                QueryModel joinedModel = model.getJoinModels().getQuick(model.getOrderedJoinModels().getQuick(i));
                if (joinedModel != model) {
                    switch (joinedModel.getJoinType()) {
                        case JOIN_OUTER:
                            sink.put(" outer join ");
                            break;
                        case JOIN_ASOF:
                            sink.put(" asof join ");
                            break;
                        case JOIN_SPLICE:
                            sink.put(" splice join ");
                            break;
                        case JOIN_CROSS:
                            sink.put(" cross join ");
                            break;
                        case JOIN_LT:
                            sink.put(" lt join ");
                            break;
                        default:
                            sink.put(" join ");
                            break;
                    }

                    if (joinedModel.getWhereClause() != null) {
                        sink.put('(');
                        nestedToSink0(sink, joinedModel);
                        sink.put(')');
                        if (joinedModel.getAlias() != null) {
                            aliasToSink(joinedModel.getAlias().token, sink);
                        } else if (model.getTableName() != null) {
                            aliasToSink(joinedModel.getTableName().token, sink);
                        }
                    } else {
                        nestedToSink0(sink, joinedModel);
                    }

                    JoinContext jc = joinedModel.getContext();
                    if (jc != null && jc.aIndexes.size() > 0) {
                        // join clause
                        sink.put(" on ");
                        for (int k = 0, z = jc.aIndexes.size(); k < z; k++) {
                            if (k > 0) {
                                sink.put(" and ");
                            }
                            jc.aNodes.getQuick(k).toSink(sink);
                            sink.put(" = ");
                            jc.bNodes.getQuick(k).toSink(sink);
                        }
                    }

                    if (joinedModel.getPostJoinWhereClause() != null) {
                        sink.put(" post-join-where ");
                        model.getPostJoinWhereClause().toSink(sink);
                    }
                }
            }
        }

        if (model.getWhereClause() != null) {
            sink.put(" where ");
            model.getWhereClause().toSink(sink);
        }

        if (model.getConstWhereClause() != null) {
            sink.put(" const-where ");
            model.getConstWhereClause().toSink(sink);
        }

        if (model.getSampleBy() != null) {
            sink.put(" sample by ");
            model.getSampleBy().toSink(sink);

            final int fillCount = model.getSampleByFill().size();
            if (fillCount > 0) {
                sink.put(" fill(");
                sink.put(model.getSampleByFill().getQuick(0));

                if (fillCount > 1) {
                    for (int i = 1; i < fillCount; i++) {
                        sink.put(',');
                        sink.put(model.getSampleByFill().getQuick(i));
                    }
                }
                sink.put(')');
            }

            if (model.getSampleByTimezoneName() != null || model.getSampleByOffset() != null) {
                sink.put(" align to calendar");
                if (model.getSampleByTimezoneName()  != null) {
                    sink.put(" time zone ");
                    sink.put(model.getSampleByTimezoneName() );
                }

                if(model.getSampleByOffset() != null) {
                    sink.put(" with offset ");
                    sink.put(model.getSampleByOffset());
                }
            }
        }

        if (model.getOrderHash().size() > 0 && model.getOrderHash().size() > 0) {
            sink.put(" order by ");

            ObjList<CharSequence> columnNames = model.getOrderHash().keys();
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }

                CharSequence key = columnNames.getQuick(i);
                sink.put(key);
                if (model.getOrderHash().get(key) == 1) {
                    sink.put(" desc");
                }
            }
        }

        if (model.getLimitLo() != null || model.getLimitHi() != null) {
            sink.put(" limit ");
            if (model.getLimitLo() != null) {
                model.getLimitLo().toSink(sink);
            }
            if (model.getLimitHi() != null) {
                sink.put(',');
                model.getLimitHi().toSink(sink);
            }
        }

        if (model.getUnionModel() != null) {
            if (model.getSetOperationType() == QueryModel.SET_OPERATION_INTERSECT) {
                sink.put(" intersect ");
            } else if (model.getSetOperationType()  == QueryModel.SET_OPERATION_EXCEPT) {
                sink.put(" except ");
            } else {
                sink.put(" union ");
                if (model.getSetOperationType()  == QueryModel.SET_OPERATION_UNION_ALL) {
                    sink.put("all ");
                }
            }
            nestedToSink0(sink, model.getUnionModel());
        }
    }

    private static void aliasToSink(CharSequence alias, CharSink sink) {
        sink.put(' ');
        boolean quote = Chars.indexOf(alias, ' ') != -1;
        if (quote) {
            sink.put('\'').put(alias).put('\'');
        } else {
            sink.put(alias);
        }
    }

    public void withSet(CharSequence col, ExpressionNode expr) {
        this.updatedColumns.add(col);
        this.updateColumnExpressions.add(expr);
    }

    public void withTableName(CharSequence tableName) {
        this.updateTableName = tableName;
    }
}
