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

package io.questdb.cutlass.text;

import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

public class TextImportRequestTask implements Mutable {
    private static final AtomicLong taskCorrelationIdGen = new AtomicLong();

    private String tableName;
    private String fileName;
    private boolean headerFlag;
    private String timestampColumnName;
    private byte delimiter;
    private String timestampFormat;
    private int partitionBy;

    private boolean cancel = false;
    private CancellationToken cancellationToken;
    private long taskCorrelationId = -1;
    private byte status;

    public static long newTaskCorrelationId() {
        return taskCorrelationIdGen.incrementAndGet();
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }

    public void setCorrelationId(long correlationId) {
       this.taskCorrelationId = correlationId;
    }

    public long getCorrelationId() {
        return this.taskCorrelationId;
    }

    public boolean isCancel() {
        return cancel;
    }

    public void setCancellationToken(@Nullable CancellationToken token) {
       this.cancellationToken = token;
    }

    public CancellationToken getCancellationToken() {
        return this.cancellationToken;
    }

    public void copyFrom(final TextImportRequestTask other) {
        this.tableName = other.tableName;
        this.fileName = other.fileName;
        this.headerFlag = other.headerFlag;
        this.timestampColumnName = other.timestampColumnName;
        this.delimiter = other.delimiter;
        this.timestampFormat = other.timestampFormat;
        this.partitionBy = other.partitionBy;
        this.cancel = other.cancel;
    }


    public void ofCancel(String tableName) {
        this.clear();
        this.tableName = tableName;
        this.cancel = true;
    }

    public void of(String tableName,
                   String fileName,
                   boolean headerFlag,
                   String timestampColumnName,
                   byte delimiter,
                   String timestampFormat,
                   int partition_by
    ) {
        this.clear();
        this.tableName = tableName;
        this.fileName = fileName;
        this.headerFlag = headerFlag;
        this.timestampColumnName = timestampColumnName;
        this.delimiter = delimiter;
        this.timestampFormat = timestampFormat;
        this.partitionBy = partition_by;
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.fileName = null;
        this.headerFlag = false;
        this.timestampColumnName = null;
        this.delimiter = 0;
        this.timestampFormat = null;
        this.partitionBy = -1;
        this.cancel = false;
    }

    public byte getDelimiter() {
        return delimiter;
    }

    public String getFileName() {
        return fileName;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public boolean isHeaderFlag() {
        return headerFlag;
    }
}
