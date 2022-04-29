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

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class CopyModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<CopyModel> FACTORY = CopyModel::new;
    private ExpressionNode tableName;
    private ExpressionNode fileName;
    private boolean header;

    private int parallelWorkersCount = 1;

    //limits memory used by parallel import to this number of bytes 
    private int memoryLimit;

    //limits number of rows imported and makes it possible to try out configuration before running full import 
    private int rowsLimit;

    private String timestampFormat;

    private int timestampColumn = -1;

    public CopyModel() {
    }

    @Override
    public void clear() {
    }

    public ExpressionNode getFileName() {
        return fileName;
    }

    public int getMemoryLimit() {
        return memoryLimit;
    }

    public int getParallelWorkersCount() {
        return parallelWorkersCount;
    }

    public int getRowsLimit() {
        return rowsLimit;
    }

    public int getTimestampColumn() {
        return timestampColumn;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setFileName(ExpressionNode fileName) {
        this.fileName = fileName;
    }

    @Override
    public int getModelType() {
        return ExecutionModel.COPY;
    }

    public ExpressionNode getTableName() {
        return tableName;
    }

    public void setMemoryLimit(int memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    public void setRowsLimit(int rowsLimit) {
        this.rowsLimit = rowsLimit;
    }

    public void setTableName(ExpressionNode tableName) {
        this.tableName = tableName;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public void setParallelWorkersCount(int workersCount) {
        this.parallelWorkersCount = workersCount;
    }

    public void setTimestampColumn(int timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    @Override
    public void toSink(CharSink sink) {

    }

    public boolean isParalell() {
        return parallelWorkersCount > 1;
    }
}
