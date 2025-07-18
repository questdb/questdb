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
    public static final int COPY_FORMAT_CSV = 1;
    public static final int COPY_FORMAT_PARQUET = 2;
    public static final int COPY_FORMAT_UNKNOWN = 0;
    public static final int COPY_TYPE_FROM = 1;
    public static final int COPY_TYPE_TO = 2;
    public static final int COPY_TYPE_UNKNOWN = 0;
    public static final ObjectFactory<CopyModel> FACTORY = CopyModel::new;
    public static String[] copyFormatMap = {"unknown", "csv", "parquet"};
    public static String[] copyTypeMap = {"unknown", "from", "to"};
    private int atomicity;
    private boolean cancel;
    private byte delimiter;
    private ExpressionNode fileName;
    private int format;
    private boolean header;
    private int partitionBy;
    private ExpressionNode target; // holds table name (new import) or import id (cancel model)
    private CharSequence timestampColumnName;
    private CharSequence timestampFormat;
    private int type;

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
        type = COPY_TYPE_UNKNOWN;
        format = COPY_FORMAT_UNKNOWN;
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

    public int getFormat() {
        return format;
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

    @Override
    public ExpressionNode getTableNameExpr() {
        return target;
    }

    public CharSequence getTimestampColumnName() {
        return timestampColumnName;
    }

    public CharSequence getTimestampFormat() {
        return timestampFormat;
    }

    public int getType() {
        return type;
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

    public void setFormat(int format) {
        this.format = format;
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

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
    }
}
