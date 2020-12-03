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
    private final ObjList<ObjList<ExpressionNode>> columnValues = new ObjList<>();
    private final IntList columnPositions = new IntList();
    private ExpressionNode tableName;
    private QueryModel queryModel;
    private int selectKeywordPosition;
    private int endOfValuesPosition;
    private ObjList<ExpressionNode> currentValueSet = null;
    private int columnValuePosition = -1;

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
        if (currentValueSet == null) {
            columnValuePosition = 0;
            addValueSet();
        }
        currentValueSet.add(value);
    }

    public void addValueSet() {
        currentValueSet = new ObjList<>();
        columnValues.add(currentValueSet);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.queryModel = null;
        this.columnSet.clear();
        this.columnPositions.clear();

        for (int i=0; i < this.columnValues.size(); i++) {
            this.columnValues.get(i).clear();
        }
        this.columnValuePosition = -1;
        this.currentValueSet = null;

        this.columnValues.clear();
        this.selectKeywordPosition = 0;
        this.endOfValuesPosition = 0;
    }

    public int getColumnPosition(int columnIndex) {
        return columnPositions.getQuick(columnIndex);
    }

    public CharSequenceHashSet getColumnSet() {
        return columnSet;
    }

    public ObjList<ExpressionNode> getColumnValues() {
        if (columnValuePosition > -1) {
            return columnValues.get(columnValuePosition);
        }
        return null;
    }

    public boolean hasNextColumnValue() {
        return (columnValuePosition != columnValues.size());
    }

    public void nextColumnValue() {
        columnValuePosition++;
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
        sink.put("insert into ").put(tableName.token).put(' ');
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
            sink.put("values ");

            for (int outer = 0; outer < columnValues.size(); outer++) {
                ObjList<ExpressionNode> valueSet = columnValues.get(outer);
                if (outer > 0) {
                    sink.put(", ");
                }
                sink.put('(');
                for (int i = 0, m = valueSet.size(); i < m; i++) {
                    if (i > 0) {
                        sink.put(", ");
                    }
                    sink.put(valueSet.getQuick(i));
                }

                sink.put(')');
            }
        }
    }
}
