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

public class CopyModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<CopyModel> FACTORY = CopyModel::new;
    private int atomicity;
    private boolean cancel;
    private byte delimiter;
    private ExpressionNode fileName;
    private boolean header;
    private int partitionBy;
    private ExpressionNode target; // holds table name (new import) or import id (cancel model)
    private CharSequence timestampColumnName;
    private CharSequence timestampFormat;

    public CopyModel() {
    }

    @Override
    public void clear() {
        target = null;
        fileName = null;
        header = false;
        cancel = false;
        timestampFormat = null;
        timestampColumnName = null;
        partitionBy = -1;
        delimiter = -1;
        atomicity = -1;
    }

    public int getAtomicity() {
        return atomicity;
    }

    public byte getDelimiter() {
        return delimiter;
    }

    public ExpressionNode getFileName() {
        return fileName;
    }

    @Override
    public int getModelType() {
        return ExecutionModel.COPY;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public CharSequence getTableName() {
        return target.token;
    }

    public ExpressionNode getTarget() {
        return target;
    }

    public CharSequence getTimestampColumnName() {
        return timestampColumnName;
    }

    public CharSequence getTimestampFormat() {
        return timestampFormat;
    }

    public boolean isCancel() {
        return cancel;
    }

    public boolean isHeader() {
        return header;
    }

    public void setAtomicity(int atomicity) {
        this.atomicity = atomicity;
    }

    public void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    public void setDelimiter(byte delimiter) {
        this.delimiter = delimiter;
    }

    public void setFileName(ExpressionNode fileName) {
        this.fileName = fileName;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setTarget(ExpressionNode tableName) {
        this.target = tableName;
    }

    public void setTimestampColumnName(CharSequence timestampColumn) {
        this.timestampColumnName = timestampColumn;
    }

    public void setTimestampFormat(CharSequence timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
    }
}
