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

package io.questdb.cutlass.parquet;


import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyExportResult;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;

public class CopyExportRequestTask implements Mutable {
    private int compressionCodec;
    private int compressionLevel;
    private CreateTableOperation createOp;
    private int dataPageSize;
    private CopyExportContext.ExportTaskEntry entry;
    private CharSequence fileName;
    private long now;
    private int nowTimestampType;
    private int parquetVersion;
    private boolean rawArrayEncoding;
    private CopyExportResult result;
    private int rowGroupSize;
    private boolean statisticsEnabled;
    private String tableName;

    @Override
    public void clear() {
        this.entry = null;
        this.tableName = null;
        this.fileName = null;
        this.compressionCodec = -1;
        this.compressionLevel = -1;
        this.dataPageSize = -1;
        this.parquetVersion = -1;
        this.rowGroupSize = -1;
        this.statisticsEnabled = true;
        this.now = 0;
        this.nowTimestampType = 0;
        this.createOp = Misc.free(createOp);
        result = null;
    }

    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return entry.getCircuitBreaker();
    }

    public int getCompressionCodec() {
        return compressionCodec;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public long getCopyID() {
        return entry.getId();
    }

    public CreateTableOperation getCreateOp() {
        return createOp;
    }

    public int getDataPageSize() {
        return dataPageSize;
    }

    public CopyExportContext.ExportTaskEntry getEntry() {
        return entry;
    }

    public CharSequence getFileName() {
        return fileName;
    }

    public long getNow() {
        return now;
    }

    public int getNowTimestampType() {
        return nowTimestampType;
    }

    public int getParquetVersion() {
        return parquetVersion;
    }

    public CopyExportResult getResult() {
        return result;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public SecurityContext getSecurityContext() {
        return entry.getSecurityContext();
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isRawArrayEncoding() {
        return rawArrayEncoding;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void of(
            CopyExportContext.ExportTaskEntry entry,
            CreateTableOperation createOp,
            CopyExportResult result,
            String tableName,
            CharSequence fileName,
            int compressionCodec,
            int compressionLevel,
            int rowGroupSize,
            int dataPageSize,
            boolean statisticsEnabled,
            int parquetVersion,
            boolean rawArrayEncoding,
            int nowTimestampType,
            long now
    ) {
        this.entry = entry;
        this.result = result;
        this.tableName = tableName;
        this.fileName = fileName;
        this.compressionCodec = compressionCodec;
        this.compressionLevel = compressionLevel;
        this.rowGroupSize = rowGroupSize;
        this.dataPageSize = dataPageSize;
        this.statisticsEnabled = statisticsEnabled;
        this.parquetVersion = parquetVersion;
        this.rawArrayEncoding = rawArrayEncoding;
        this.createOp = createOp;
        this.now = now;
        this.nowTimestampType = nowTimestampType;
    }

    public enum Phase {
        NONE(""),
        WAITING("wait_to_run"),
        POPULATING_TEMP_TABLE("populating_data_to_temp_table"),
        CONVERTING_PARTITIONS("converting_partitions"),
        MOVE_FILES("move_files"),
        DROPPING_TEMP_TABLE("dropping_temp_table"),
        SENDING_DATA("sending_data"),
        SUCCESS("success");

        private final String name;

        Phase(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public enum Status {
        NONE(""),
        STARTED("started"),
        FINISHED("finished"),
        FAILED("failed"),
        CANCELLED("cancelled");

        private final String name;

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
