/*+*****************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class CreateTableColumnModel implements Mutable {
    public static final ObjectFactory<CreateTableColumnModel> FACTORY = CreateTableColumnModel::new;
    private int columnNamePos = -1;
    private int columnType = ColumnType.UNDEFINED;
    private int columnTypePos = -1;
    private int dedupColumnPos = -1;
    private boolean dedupKeyFlag;
    private int indexColumnPos = -1;
    private int indexValueBlockSize;
    private boolean indexedFlag;
    private boolean isCast;
    private boolean parquetBloomFilter;
    private int parquetCompression = -1;
    private int parquetCompressionLevel = -1;
    private int parquetEncoding = -1;
    private boolean symbolCacheFlag;
    private int symbolCapacity = -1;

    private CreateTableColumnModel() {
        clear();
    }

    @Override
    public void clear() {
        columnNamePos = -1;
        columnType = ColumnType.UNDEFINED;
        columnTypePos = -1;
        dedupKeyFlag = false;
        dedupColumnPos = -1;
        indexColumnPos = -1;
        indexValueBlockSize = 0;
        indexedFlag = false;
        isCast = false;
        parquetBloomFilter = false;
        parquetCompression = -1;
        parquetCompressionLevel = -1;
        parquetEncoding = -1;
        symbolCacheFlag = false;
        symbolCapacity = -1;
    }

    public int getColumnNamePos() {
        return columnNamePos;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getColumnTypePos() {
        return columnTypePos;
    }

    public int getDedupColumnPos() {
        return dedupColumnPos;
    }

    public int getIndexColumnPos() {
        return indexColumnPos;
    }

    public int getIndexValueBlockSize() {
        return indexValueBlockSize;
    }

    public boolean isParquetBloomFilter() {
        return parquetBloomFilter;
    }

    public int getParquetCompression() {
        return parquetCompression;
    }

    public int getParquetCompressionLevel() {
        return parquetCompressionLevel;
    }

    public int getParquetEncoding() {
        return parquetEncoding;
    }

    public int getParquetEncodingConfig() {
        if (parquetEncoding < 0 && parquetCompression < 0 && !parquetBloomFilter) {
            return 0;
        }
        // In packed form, compression is shifted +1 (0=default, 1=uncompressed, 2=snappy, etc.)
        // to distinguish "not set" from "explicitly uncompressed".
        int packedCompression = parquetCompression >= 0 ? parquetCompression + 1 : 0;
        // Level is also shifted +1 (0=not set, 1=level 0, 2=level 1, etc.)
        // to distinguish "not set" from "level 0" (e.g., gzip store mode).
        int packedLevel = parquetCompressionLevel >= 0 ? parquetCompressionLevel + 1 : 0;
        return TableUtils.packParquetConfig(
                Math.max(parquetEncoding, 0),
                packedCompression,
                packedLevel,
                parquetBloomFilter
        );
    }

    public boolean getSymbolCacheFlag() {
        return symbolCacheFlag;
    }

    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public boolean isCast() {
        return isCast;
    }

    public boolean isDedupKey() {
        return dedupKeyFlag;
    }

    public boolean isIndexed() {
        return indexedFlag;
    }

    public void setCastType(int columnType, int columnTypePos) {
        this.isCast = true;
        this.columnType = columnType;
        this.columnTypePos = columnTypePos;
    }

    public void setColumnNamePos(int columnNamePos) {
        this.columnNamePos = columnNamePos;
    }

    public void setColumnType(int columnType) {
        this.columnType = columnType;
    }

    public void setIndexed(boolean indexedFlag, int indexColumnPosition, int indexValueBlockSize) {
        this.indexedFlag = indexedFlag;
        this.indexColumnPos = indexColumnPosition;
        this.indexValueBlockSize = indexValueBlockSize;
    }

    public void setIsDedupKey() {
        dedupKeyFlag = true;
    }

    public void setParquetBloomFilter(boolean parquetBloomFilter) {
        this.parquetBloomFilter = parquetBloomFilter;
    }

    public void setParquetCompression(int parquetCompression) {
        this.parquetCompression = parquetCompression;
    }

    public void setParquetCompressionLevel(int parquetCompressionLevel) {
        this.parquetCompressionLevel = parquetCompressionLevel;
    }

    public void setParquetEncoding(int parquetEncoding) {
        this.parquetEncoding = parquetEncoding;
    }

    /**
     * Sets all parquet properties from a packed config int produced by
     * {@link TableUtils#packParquetConfig(int, int, int, boolean)}.
     * Unpacks encoding, compression (+1 encoded), level (+1 encoded), and bloom filter flag
     * into the individual fields.
     */
    public void setParquetEncodingConfig(int packed) {
        int enc = TableUtils.getParquetConfigEncoding(packed);
        this.parquetEncoding = enc > 0 ? enc : -1;
        int comp = TableUtils.getParquetConfigCompression(packed);
        this.parquetCompression = comp > 0 ? comp - 1 : -1;
        int lvl = TableUtils.getParquetConfigCompressionLevel(packed);
        this.parquetCompressionLevel = lvl > 0 ? lvl - 1 : -1;
        this.parquetBloomFilter = TableUtils.isParquetConfigBloomFilter(packed);
    }

    public void setSymbolCacheFlag(boolean symbolCacheFlag) {
        this.symbolCacheFlag = symbolCacheFlag;
    }

    public void setSymbolCapacity(int symbolCapacity) {
        this.symbolCapacity = symbolCapacity;
    }
}
