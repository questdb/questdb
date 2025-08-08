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

import io.questdb.std.LowerCaseCharSequenceIntHashMap;

public class ParquetCompression {
    private static final LowerCaseCharSequenceIntHashMap compressionCodecNames = new LowerCaseCharSequenceIntHashMap();
    public static int COMPRESSION_BROTLI = 4;
    public static int COMPRESSION_GZIP = 2;
    public static int COMPRESSION_LZ4 = 5;
    public static int COMPRESSION_LZ4_RAW = 7;
    public static int COMPRESSION_LZO = 3;
    public static int COMPRESSION_SNAPPY = 1;
    public static int COMPRESSION_UNCOMPRESSED = 0;
    public static int COMPRESSION_ZSTD = 6;

    public static int getCompressionCodec(CharSequence name) {
        return compressionCodecNames.get(name);
    }

    public static long packCompressionCodecLevel(int compression, long level) {
        return (level << 32) | compression;
    }

    static {
        compressionCodecNames.put("uncompressed", COMPRESSION_UNCOMPRESSED);
        compressionCodecNames.put("zstd", COMPRESSION_ZSTD);
        compressionCodecNames.put("lz4", COMPRESSION_LZ4);
        compressionCodecNames.put("gzip", COMPRESSION_GZIP);
        compressionCodecNames.put("lz4_raw", COMPRESSION_LZ4_RAW);
        compressionCodecNames.put("lzo", COMPRESSION_LZO);
        compressionCodecNames.put("snappy", COMPRESSION_SNAPPY);
        compressionCodecNames.put("brotli", COMPRESSION_BROTLI);
    }
}
