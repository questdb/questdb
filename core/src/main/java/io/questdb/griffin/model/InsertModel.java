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

import io.questdb.griffin.SqlException;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class InsertModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<InsertModel> FACTORY = InsertModel::new;
    private final ObjList<CharSequence> columnNameList = new ObjList<>();
    private final LowerCaseCharSequenceHashSet columnNameSet = new LowerCaseCharSequenceHashSet();
    private final IntList columnPositions = new IntList();
    private final IntList endOfRowTupleValuesPositions = new IntList();
    private final ObjList<ObjList<ExpressionNode>> rowTupleValues = new ObjList<>();
    private long batchSize = -1;
    private long commitLag = 0;
    private QueryModel queryModel;
    private int selectKeywordPosition;
    private ExpressionNode tableName;

    private InsertModel() {
    }

    public void addColumn(CharSequence columnName, int columnPosition) throws SqlException {
        int keyIndex = columnNameSet.keyIndex(columnName);
        if (keyIndex > -1) {
            String name = Chars.toString(columnName);
            columnNameSet.addAt(keyIndex, name);
            columnPositions.add(columnPosition);
            columnNameList.add(name);
            return;
        }
        throw SqlException.duplicateColumn(columnPosition, columnName);
    }

    public void addEndOfRowTupleValuesPosition(int endOfValuesPosition) {
        endOfRowTupleValuesPositions.add(endOfValuesPosition);
    }

    public void addRowTupleValues(ObjList<ExpressionNode> row) {
        rowTupleValues.add(row);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.queryModel = null;
        this.columnNameSet.clear();
        this.columnPositions.clear();
        this.columnNameList.clear();
        for (int i = 0, n = this.rowTupleValues.size(); i < n; i++) {
            this.rowTupleValues.get(i).clear();
        }
        this.rowTupleValues.clear();
        this.selectKeywordPosition = 0;
        this.endOfRowTupleValuesPositions.clear();
        this.batchSize = -1;
        this.commitLag = 0;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public ObjList<CharSequence> getColumnNameList() {
        return columnNameList;
    }

    public int getColumnPosition(int columnIndex) {
        return columnPositions.getQuick(columnIndex);
    }

    public long getCommitLag() {
        return commitLag;
    }

    public int getEndOfRowTupleValuesPosition(int index) {
        return endOfRowTupleValuesPositions.get(index);
    }

    @Override
    public int getModelType() {
        return INSERT;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public int getRowTupleCount() {
        return rowTupleValues.size();
    }

    public ObjList<ExpressionNode> getRowTupleValues(int index) {
        return rowTupleValues.get(index);
    }

    public int getSelectKeywordPosition() {
        return selectKeywordPosition;
    }

    public ExpressionNode getTableName() {
        return tableName;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void setCommitLag(long lag) {
        this.commitLag = lag;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public void setSelectKeywordPosition(int selectKeywordPosition) {
        this.selectKeywordPosition = selectKeywordPosition;
    }

    public void setTableName(ExpressionNode tableName) {
        this.tableName = tableName;
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
        int n = columnNameList.size();
        if (n > 0) {
            sink.put('(');
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(columnNameList.getQuick(i));
            }
            sink.put(") ");
        }
        if (queryModel != null) {
            queryModel.toSink(sink);
        } else {
            sink.put("values ");
            for (int t = 0, s = rowTupleValues.size(); t < s; t++) {
                ObjList<ExpressionNode> rowValues = rowTupleValues.get(t);
                sink.put('(');
                for (int i = 0, m = rowValues.size(); i < m; i++) {
                    if (i > 0) {
                        sink.put(", ");
                    }
                    sink.put(rowValues.getQuick(i));
                }
                sink.put(')');
            }
        }
    }
}
