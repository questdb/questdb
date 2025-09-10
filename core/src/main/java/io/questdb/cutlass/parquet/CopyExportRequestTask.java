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

package io.questdb.cutlass.parquet;


import io.questdb.cairo.SecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.network.SuspendEvent;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

public class CopyExportRequestTask implements Mutable {
    private int compressionCodec;
    private int compressionLevel;
    private long copyID;
    private CreateTableOperation createOp;
    private int dataPageSize;
    private SqlExecutionContext executionContext;
    private String fileName;
    private int parquetVersion;
    private boolean rawArrayEncoding;
    private int rowGroupSize;
    private SecurityContext securityContext;
    private int sizeLimit;
    private boolean statisticsEnabled;
    private @Nullable SuspendEvent suspendEvent;
    private String tableName;

    @Override
    public void clear() {
        this.copyID = -1;
        this.tableName = null;
        this.fileName = null;
        this.securityContext = null;
        this.compressionCodec = -1;
        this.compressionLevel = -1;
        this.dataPageSize = -1;
        this.parquetVersion = -1;
        this.rowGroupSize = -1;
        this.sizeLimit = -1;
        this.statisticsEnabled = true;
        this.suspendEvent = null;
        this.createOp = Misc.free(createOp);
        this.executionContext = null;
    }

    public int getCompressionCodec() {
        return compressionCodec;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public long getCopyID() {
        return copyID;
    }

    public CreateTableOperation getCreateOp() {
        return createOp;
    }

    public int getDataPageSize() {
        return dataPageSize;
    }

    public SqlExecutionContext getExecutionContext() {
        return executionContext;
    }

    public String getFileName() {
        return fileName;
    }

    public int getParquetVersion() {
        return parquetVersion;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public int getSizeLimit() {
        return sizeLimit;
    }

    public @Nullable SuspendEvent getSuspendEvent() {
        return suspendEvent;
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
            SecurityContext securityContext,
            SqlExecutionContext sqlExecutionContext,
            long copyID,
            CreateTableOperation createOp,
            String tableName,
            String fileName,
            int sizeLimit,
            int compressionCodec,
            int compressionLevel,
            int rowGroupSize,
            int dataPageSize,
            boolean statisticsEnabled,
            int parquetVersion,
            @Nullable SuspendEvent suspendEvent,
            boolean rawArrayEncoding
    ) {
        this.clear();
        this.executionContext = sqlExecutionContext;
        this.securityContext = securityContext;
        this.copyID = copyID;
        this.tableName = tableName;
        this.fileName = fileName;
        this.sizeLimit = sizeLimit;
        this.compressionCodec = compressionCodec;
        this.compressionLevel = compressionLevel;
        this.rowGroupSize = rowGroupSize;
        this.dataPageSize = dataPageSize;
        this.statisticsEnabled = statisticsEnabled;
        this.parquetVersion = parquetVersion;
        this.rawArrayEncoding = rawArrayEncoding;
        this.suspendEvent = suspendEvent;
        this.createOp = createOp;
    }

    public enum Phase {
        NONE((byte) -1, null),
        CREATING_TEMP_TABLE((byte) 0, "creating_temp_table"),
        CONVERTING_PARTITIONS((byte) 1, "converting_partitions"),
        DROPPING_TEMP_TABLE((byte) 2, "dropping_temp_table"),
        SIGNALLING_EXP((byte) 3, "signalling_exp");

        private final String name;
        private final byte value;

        Phase(byte value, String name) {
            this.value = value;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public byte getValue() {
            return value;
        }
    }

    public enum Status {
        STARTED((byte) 0, "started"),
        FINISHED((byte) 1, "finished"),
        FAILED((byte) 2, "failed"),
        CANCELLED((byte) 3, "cancelled"),
        PENDING((byte) 4, "pending");

        private final String name;
        private final byte value;

        Status(byte value, String name) {
            this.value = value;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public byte getValue() {
            return value;
        }
    }

}
