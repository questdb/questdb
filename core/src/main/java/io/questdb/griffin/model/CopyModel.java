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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CopyModel implements ExecutionModel, Mutable, Sinkable {
    public static final int COPY_FORMAT_CSV = 1;
    public static final int COPY_FORMAT_PARQUET = 2;
    public static final int COPY_FORMAT_UNKNOWN = 0;
    public static final int COPY_OPTION_UNKNOWN = -1;
    public static final int COPY_OPTION_FORMAT = COPY_OPTION_UNKNOWN + 1; // 0
    public static final int COPY_OPTION_PARTITION_BY = COPY_OPTION_FORMAT + 1; // 1
    public static final int COPY_OPTION_SIZE_LIMIT = COPY_OPTION_PARTITION_BY + 1; // 2
    public static final int COPY_OPTION_COMPRESSION_CODEC = COPY_OPTION_SIZE_LIMIT + 1; // 3
    public static final int COPY_OPTION_COMPRESSION_LEVEL = COPY_OPTION_COMPRESSION_CODEC + 1; // 4
    public static final int COPY_OPTION_ROW_GROUP_SIZE = COPY_OPTION_COMPRESSION_LEVEL + 1; // 5
    public static final int COPY_OPTION_DATA_PAGE_SIZE = COPY_OPTION_ROW_GROUP_SIZE + 1; // 6
    public static final int COPY_OPTION_STATISTICS_ENABLED = COPY_OPTION_DATA_PAGE_SIZE + 1; // 7
    public static final int COPY_OPTION_PARQUET_VERSION = COPY_OPTION_STATISTICS_ENABLED + 1; // 8
    public static final int COPY_TYPE_FROM = 1;
    public static final int COPY_TYPE_TO = 2;
    public static final int COPY_TYPE_UNKNOWN = 0;
    public static final ObjectFactory<CopyModel> FACTORY = CopyModel::new;
    private static final LowerCaseCharSequenceIntHashMap copyOptionsNameToEnumMap = new LowerCaseCharSequenceIntHashMap();
    public static String[] copyFormatMap = {"unknown", "csv", "parquet"};
    public static String[] copyTypeMap = {"unknown", "from", "to"};
    @Nullable
    private int atomicity;
    private boolean cancel;
    private int compressionCodec;
    private int compressionLevel;
    private int dataPageSize;
    private byte delimiter;
    private ExpressionNode fileName;
    private int format;
    private boolean header;
    private int parquetVersion;
    private int partitionBy;
    private int rowGroupSize;
    @Nullable
    private String selectText;
    private int sizeLimit;
    private boolean statisticsEnabled;
    private ExpressionNode target; // holds table name (new import) or import id (cancel model)
    private CharSequence timestampColumnName;
    private CharSequence timestampFormat;
    private int type;


    public CopyModel() {
    }

    public static int getExportOption(CharSequence tok) {
        return copyOptionsNameToEnumMap.get(tok);
    }

    @Override
    public void clear() {
        target = null;
        fileName = null;
        header = false;
        cancel = false;
        timestampFormat = null;
        timestampColumnName = null;
        partitionBy = PartitionBy.NONE;
        delimiter = -1;
        atomicity = -1;
        type = COPY_TYPE_UNKNOWN;
        format = COPY_FORMAT_UNKNOWN;
        selectText = null;
        compressionCodec = -1;
        compressionLevel = -1;
        rowGroupSize = -1;
        dataPageSize = -1;
        parquetVersion = -1;
        statisticsEnabled = true;
        sizeLimit = -1;
    }

    public int getAtomicity() {
        return atomicity;
    }

    public int getCompressionCodec() {
        return compressionCodec;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public int getDataPageSize() {
        return dataPageSize;
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

    public int getParquetVersion() {
        return parquetVersion;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    @Override
    public @Nullable String getSelectText() {
        return selectText;
    }

    public int getSizeLimit() {
        return sizeLimit;
    }

    @Override
    public @Nullable CharSequence getTableName() {
        return target != null ? target.token : null;
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

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setAtomicity(int atomicity) {
        this.atomicity = atomicity;
    }

    public void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    public void setCompressionCodec(int compressionCodec) {
        this.compressionCodec = compressionCodec;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public void setDataPageSize(int dataPageSize) {
        this.dataPageSize = dataPageSize;
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

    public void setParquetDefaults(CairoConfiguration configuration) {
        compressionCodec = configuration.getPartitionEncoderParquetCompressionCodec();
        compressionLevel = configuration.getPartitionEncoderParquetCompressionLevel();
        rowGroupSize = configuration.getPartitionEncoderParquetRowGroupSize();
        dataPageSize = configuration.getPartitionEncoderParquetDataPageSize();
        statisticsEnabled = configuration.isPartitionEncoderParquetStatisticsEnabled();
        parquetVersion = configuration.getPartitionEncoderParquetVersion();
    }

    public void setParquetVersion(int parquetVersion) {
        this.parquetVersion = parquetVersion;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setRowGroupSize(int rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public void setSelectText(@Nullable String selectText) {
        this.selectText = selectText;
    }

    public void setSizeLimit(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
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

    static {
        copyOptionsNameToEnumMap.put("format", COPY_OPTION_FORMAT);
        copyOptionsNameToEnumMap.put("partition_by", COPY_OPTION_PARTITION_BY);
        copyOptionsNameToEnumMap.put("size_limit", COPY_OPTION_SIZE_LIMIT);
        copyOptionsNameToEnumMap.put("compression_codec", COPY_OPTION_COMPRESSION_CODEC);
        copyOptionsNameToEnumMap.put("compression_level", COPY_OPTION_COMPRESSION_LEVEL);
        copyOptionsNameToEnumMap.put("row_group_size", COPY_OPTION_ROW_GROUP_SIZE);
        copyOptionsNameToEnumMap.put("data_page_size", COPY_OPTION_DATA_PAGE_SIZE);
        copyOptionsNameToEnumMap.put("statistics_enabled", COPY_OPTION_STATISTICS_ENABLED);
        copyOptionsNameToEnumMap.put("parquet_version", COPY_OPTION_PARQUET_VERSION);
    }
}
