/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class InsertModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<InsertModel> FACTORY = InsertModel::new;
    private final CharSequenceHashSet columnSet = new CharSequenceHashSet();
    private final ObjList<ExpressionNode> columnValues = new ObjList<>();
    private final IntList columnPositions = new IntList();
    private ExpressionNode tableName;
    private QueryModel queryModel;
    private int selectKeywordPosition;
    private int endOfValuesPosition;
    private long batchSize = -1;
    private long commitLag = 0;

    private InsertModel() {
    }

    public boolean addColumn(CharSequence columnName, int columnPosition) {
        if (columnSet.add(columnName)) {
            columnPositions.add(columnPosition);
            return true;
        }
        return false;
    }

    public void addColumnValue(ExpressionNode value) {
        columnValues.add(value);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.queryModel = null;
        this.columnSet.clear();
        this.columnPositions.clear();
        this.columnValues.clear();
        this.selectKeywordPosition = 0;
        this.endOfValuesPosition = 0;
        this.batchSize = -1;
        this.commitLag = 0;
    }

    public int getColumnPosition(int columnIndex) {
        return columnPositions.getQuick(columnIndex);
    }

    public CharSequenceHashSet getColumnSet() {
        return columnSet;
    }

    public ObjList<ExpressionNode> getColumnValues() {
        return columnValues;
    }

    public int getSelectKeywordPosition() {
        return selectKeywordPosition;
    }

    public void setSelectKeywordPosition(int selectKeywordPosition) {
        this.selectKeywordPosition = selectKeywordPosition;
    }

    @Override
    public int getModelType() {
        return INSERT;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public void setCommitLag(long lag) {
        this.commitLag = lag;
    }

    public ExpressionNode getTableName() {
        return tableName;
    }

    public void setTableName(ExpressionNode tableName) {
        this.tableName = tableName;
    }

    public int getEndOfValuesPosition() {
        return endOfValuesPosition;
    }

    public void setEndOfValuesPosition(int endOfValuesPosition) {
        this.endOfValuesPosition = endOfValuesPosition;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("insert");
        if (batchSize != -1) {
            sink.put(" batch ").put(batchSize);
        }

        if (commitLag != 0) {
            sink.put(" lag ").put(commitLag);
        }

        sink.put(" into ").put(tableName.token).put(' ');
        int n = columnSet.size();
        if (n > 0) {
            sink.put('(');
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(columnSet.get(i));
            }
            sink.put(") ");
        }
        if (queryModel != null) {
            queryModel.toSink(sink);
        } else {
            sink.put("values (");

            for (int i = 0, m = columnValues.size(); i < m; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(columnValues.getQuick(i));
            }

            sink.put(')');
        }
    }
}
