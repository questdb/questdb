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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.griffin.SqlException;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;

public class ParquetCompression {
    private static final IntObjHashMap<CharSequence> codecToNameMap = new IntObjHashMap<>();
    private static final LowerCaseCharSequenceIntHashMap nameToCodecMap = new LowerCaseCharSequenceIntHashMap();
    public static int COMPRESSION_UNCOMPRESSED = 0; // 0
    public static int COMPRESSION_SNAPPY = COMPRESSION_UNCOMPRESSED + 1; // 1
    public static int COMPRESSION_GZIP = COMPRESSION_SNAPPY + 1; // 2
    public static int COMPRESSION_LZO = COMPRESSION_GZIP + 1; // 3
    public static int COMPRESSION_BROTLI = COMPRESSION_LZO + 1; // 4
    public static int COMPRESSION_LZ4 = COMPRESSION_BROTLI + 1; // 5
    public static int COMPRESSION_ZSTD = COMPRESSION_LZ4 + 1; // 6
    public static int COMPRESSION_LZ4_RAW = COMPRESSION_ZSTD + 1; // 7
    public static int COMPRESSION_DEFAULT = COMPRESSION_LZ4_RAW;
    static int MAX_ENUM_INT = COMPRESSION_LZ4_RAW + 1;

    public static void addCodecNamesToException(SqlException e) {
        for (int i = 0, n = MAX_ENUM_INT; i < n; i++) {
            e.put(codecToNameMap.get(i));
            if (i + 1 != n) {
                e.put(", ");
            }
        }
    }

    public static int getCompressionCodec(CharSequence name) {
        return nameToCodecMap.get(name);
    }

    public static long packCompressionCodecLevel(int compression, long level) {
        return (level << 32) | compression;
    }

    static {
        nameToCodecMap.put("uncompressed", COMPRESSION_UNCOMPRESSED);
        nameToCodecMap.put("zstd", COMPRESSION_ZSTD);
        nameToCodecMap.put("lz4", COMPRESSION_LZ4);
        nameToCodecMap.put("gzip", COMPRESSION_GZIP);
        nameToCodecMap.put("lz4_raw", COMPRESSION_LZ4_RAW);
        nameToCodecMap.put("lzo", COMPRESSION_LZO);
        nameToCodecMap.put("snappy", COMPRESSION_SNAPPY);
        nameToCodecMap.put("brotli", COMPRESSION_BROTLI);
        nameToCodecMap.put("default", COMPRESSION_LZ4_RAW);

        codecToNameMap.put(COMPRESSION_UNCOMPRESSED, "uncompressed");
        codecToNameMap.put(COMPRESSION_ZSTD, "zstd");
        codecToNameMap.put(COMPRESSION_LZ4, "lz4");
        codecToNameMap.put(COMPRESSION_GZIP, "gzip");
        codecToNameMap.put(COMPRESSION_LZ4_RAW, "lz4_raw");
        codecToNameMap.put(COMPRESSION_LZO, "lzo");
        codecToNameMap.put(COMPRESSION_SNAPPY, "snappy");
        codecToNameMap.put(COMPRESSION_BROTLI, "brotli");
    }
}
