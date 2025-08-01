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
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Mutable;

public class CopyExportRequestTask implements Mutable {
    public static final byte PHASE_NONE = -1; // -1
    public static final byte PHASE_CREATING_TEMP_TABLE = PHASE_NONE + 1; // 0
    public static final byte PHASE_CONVERTING_PARTITIONS = PHASE_CREATING_TEMP_TABLE + 1; // 1
    public static final byte PHASE_DROPPING_TEMP_TABLE = PHASE_CONVERTING_PARTITIONS + 1; // 2
    public static final byte STATUS_STARTED = 0; // 0
    public static final byte STATUS_FINISHED = STATUS_STARTED + 1; // 1
    public static final byte STATUS_FAILED = STATUS_FINISHED + 1; // 2
    public static final byte STATUS_CANCELLED = STATUS_FAILED + 1; // 3
    public static final byte STATUS_PENDING = STATUS_CANCELLED + 1; // 4
    private static final IntObjHashMap<String> PHASE_NAME_MAP = new IntObjHashMap<>();
    private static final IntObjHashMap<String> STATUS_NAME_MAP = new IntObjHashMap<>();
    private int compressionCodec;
    private int compressionLevel;
    private long copyID;
    private int dataPageSize;
    private String fileName;
    private int parquetVersion;
    private int rowGroupSize;
    private SecurityContext securityContext;
    private int sizeLimit;
    private boolean statisticsEnabled;
    private String tableName;

    public static String getPhaseName(byte status) {
        return PHASE_NAME_MAP.get(status);
    }

    public static String getStatusName(byte status) {
        return STATUS_NAME_MAP.get(status);
    }

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

    public int getDataPageSize() {
        return dataPageSize;
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

    public String getTableName() {
        return tableName;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void of(
            SecurityContext securityContext,
            long copyID,
            String tableName,
            String fileName,
            int sizeLimit,
            int compressionCodec,
            int compressionLevel,
            int rowGroupSize,
            int dataPageSize,
            boolean statisticsEnabled,
            int parquetVersion
    ) {
        this.clear();
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
    }

    static {
        PHASE_NAME_MAP.put(PHASE_NONE, null);
        PHASE_NAME_MAP.put(PHASE_CREATING_TEMP_TABLE, "creating_temp_table");
        PHASE_NAME_MAP.put(PHASE_CONVERTING_PARTITIONS, "converting_partitions");
        PHASE_NAME_MAP.put(PHASE_DROPPING_TEMP_TABLE, "dropping_temp_table");
        STATUS_NAME_MAP.put(STATUS_STARTED, "started");
        STATUS_NAME_MAP.put(STATUS_FINISHED, "finished");
        STATUS_NAME_MAP.put(STATUS_FAILED, "failed");
        STATUS_NAME_MAP.put(STATUS_CANCELLED, "cancelled");
        STATUS_NAME_MAP.put(STATUS_PENDING, "pending");
    }
}
