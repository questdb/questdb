/*******************************************************************************
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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.parquet.ParquetCompression.*;

public class ExportModel implements ExecutionModel, Mutable, Sinkable {
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
    public static final int COPY_OPTION_RAW_ARRAY_ENCODING = COPY_OPTION_PARQUET_VERSION + 1; // 9
    public static final int COPY_TYPE_FROM = 1;
    public static final int COPY_TYPE_TO = 2;
    public static final int COPY_TYPE_UNKNOWN = 0;
    public static final ObjectFactory<ExportModel> FACTORY = ExportModel::new;
    public static final int PARQUET_VERSION_V1 = 1;
    public static final int PARQUET_VERSION_V2 = 2;
    private static final LowerCaseCharSequenceIntHashMap copyOptionsNameToEnumMap = new LowerCaseCharSequenceIntHashMap();
    private int atomicity = -1;
    private boolean cancel;
    private int compressionCodec = -1;
    private int compressionLevel = -1;
    private int compressionLevelPos;
    private boolean compressionLevelSet;
    private int dataPageSize = -1;
    private byte delimiter = -1;
    private ExpressionNode fileName;
    private int format;
    private boolean header;
    private boolean noDelay;
    private int parquetVersion = -1;
    private int partitionBy = -1;
    private boolean rawArrayEncoding;
    private int rowGroupSize = -1;
    private int selectStartPos;
    @Nullable
    private CharSequence selectText;
    private boolean statisticsEnabled = true;
    private ExpressionNode target; // holds table name (new import) or import id (cancel model)
    private CharSequence timestampColumnName;
    private CharSequence timestampFormat;
    private int type;

    public ExportModel() {
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
        partitionBy = -1;
        selectStartPos = 0;
        delimiter = -1;
        atomicity = -1;
        type = COPY_TYPE_UNKNOWN;
        format = COPY_FORMAT_UNKNOWN;
        selectText = null;
        compressionCodec = -1;
        compressionLevel = -1;
        compressionLevelSet = false;
        rowGroupSize = -1;
        dataPageSize = -1;
        parquetVersion = -1;
        statisticsEnabled = true;
        rawArrayEncoding = false;
        compressionLevelPos = 0;
        noDelay = false;
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
    public @Nullable CharSequence getSelectText() {
        return selectText;
    }

    public int getSelectTextStartPos() {
        return selectStartPos;
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

    public void initDefaultCompressLevel() {
        if (format == COPY_FORMAT_PARQUET && compressionCodec >= 0 && !compressionLevelSet) {
            switch (compressionCodec) {
                case ParquetCompression.COMPRESSION_GZIP:
                    if (compressionLevel < GZIP_MIN_COMPRESSION_LEVEL || compressionLevel > GZIP_MAX_COMPRESSION_LEVEL) {
                        compressionLevel = GZIP_MIN_COMPRESSION_LEVEL;
                    }
                    break;
                case ParquetCompression.COMPRESSION_BROTLI:
                    if (compressionLevel < BROTLI_MIN_COMPRESSION_LEVEL || compressionLevel > BROTLI_MAX_COMPRESSION_LEVEL) {
                        compressionLevel = BROTLI_MIN_COMPRESSION_LEVEL;
                    }
                    break;
                case ParquetCompression.COMPRESSION_ZSTD:
                    if (compressionLevel < ZSTD_MIN_COMPRESSION_LEVEL || compressionLevel > ZSTD_MAX_COMPRESSION_LEVEL) {
                        compressionLevel = ZSTD_MIN_COMPRESSION_LEVEL;
                    }
                    break;
            }
        }
    }

    public boolean isCancel() {
        return cancel;
    }

    public boolean isHeader() {
        return header;
    }

    public boolean isNoDelay() {
        return noDelay;
    }

    public boolean isParquetFormat() {
        return format == COPY_FORMAT_PARQUET;
    }

    public boolean isRawArrayEncoding() {
        return rawArrayEncoding;
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

    public void setCompressionLevel(int compressionLevel, int pos) {
        this.compressionLevel = compressionLevel;
        this.compressionLevelPos = pos;
        this.compressionLevelSet = true;
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

    public void setNoDelayTo(boolean noDelay) {
        this.noDelay = noDelay;
    }

    public void setParquetDefaults(CairoConfiguration configuration) {
        compressionCodec = configuration.getParquetExportCompressionCodec();
        compressionLevel = configuration.getParquetExportCompressionLevel();
        rowGroupSize = configuration.getParquetExportRowGroupSize();
        dataPageSize = configuration.getParquetExportDataPageSize();
        statisticsEnabled = configuration.isParquetExportStatisticsEnabled();
        parquetVersion = configuration.getParquetExportVersion();
        rawArrayEncoding = configuration.isParquetExportRawArrayEncoding();
        partitionBy = -1;
        compressionLevelSet = false;
    }

    public void setParquetVersion(int parquetVersion) {
        this.parquetVersion = parquetVersion;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setRawArrayEncoding(boolean rawArrayEncoding) {
        this.rawArrayEncoding = rawArrayEncoding;
    }

    public void setRowGroupSize(int rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public void setSelectText(@Nullable CharSequence selectText, int startPos) {
        this.selectText = selectText;
        this.selectStartPos = startPos;
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

    public void validCompressOptions() throws SqlException {
        if (!compressionLevelSet) {
            initDefaultCompressLevel();
            return;
        }
        if (format == COPY_FORMAT_PARQUET && compressionCodec >= 0) {
            switch (compressionCodec) {
                case COMPRESSION_UNCOMPRESSED:
                case COMPRESSION_SNAPPY:
                case COMPRESSION_LZ4_RAW:
                    // These codecs don't use compression level
                    break;
                case COMPRESSION_GZIP:
                    // GZIP actually uses levels 0-9, where 0=fastest, 9=best compression
                    if (compressionLevel < GZIP_MIN_COMPRESSION_LEVEL || compressionLevel > GZIP_MAX_COMPRESSION_LEVEL) {
                        throw SqlException.$(compressionLevelPos, "GZIP compression level must be between 0 and 9");
                    }
                    break;
                case COMPRESSION_BROTLI:
                    if (compressionLevel < BROTLI_MIN_COMPRESSION_LEVEL || compressionLevel > BROTLI_MAX_COMPRESSION_LEVEL) {
                        throw SqlException.$(compressionLevelPos, "Brotli compression level must be between 0 and 11");
                    }
                    break;
                case COMPRESSION_ZSTD:
                    if (compressionLevel != -1 && (compressionLevel < ZSTD_MIN_COMPRESSION_LEVEL || compressionLevel > ZSTD_MAX_COMPRESSION_LEVEL)) {
                        throw SqlException.$(compressionLevelPos, "ZSTD compression level must be between 1 and 22");
                    }
                    break;
                default:
                    throw SqlException.$(compressionLevelPos, "Unsupported compression codec ").put(compressionCodec);
            }
        }
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
        copyOptionsNameToEnumMap.put("raw_array_encoding", COPY_OPTION_RAW_ARRAY_ENCODING);
    }
}
