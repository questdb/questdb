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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.griffin.SqlException;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.str.StringSink;

public class ParquetCompression {
    public static final int BROTLI_MAX_COMPRESSION_LEVEL = 11;
    public static final int BROTLI_MIN_COMPRESSION_LEVEL = 0;
    public static final int COMPRESSION_UNCOMPRESSED = 0; // 0
    public static final int COMPRESSION_SNAPPY = COMPRESSION_UNCOMPRESSED + 1; // 1
    public static final int COMPRESSION_GZIP = COMPRESSION_SNAPPY + 1; // 2
    public static final int COMPRESSION_BROTLI = COMPRESSION_GZIP + 1; // 3
    public static final int COMPRESSION_ZSTD = COMPRESSION_BROTLI + 1; // 4
    public static final int COMPRESSION_LZ4_RAW = COMPRESSION_ZSTD + 1; // 5
    static final int MAX_ENUM_INT = COMPRESSION_LZ4_RAW + 1;
    public static final int GZIP_MAX_COMPRESSION_LEVEL = 9;
    public static final int GZIP_MIN_COMPRESSION_LEVEL = 0;
    public static final int WRITER_COMPRESSION_UNCOMPRESSED = 0; // 0
    public static final int WRITER_COMPRESSION_SNAPPY = WRITER_COMPRESSION_UNCOMPRESSED + 1; // 1
    public static final int WRITER_COMPRESSION_GZIP = WRITER_COMPRESSION_SNAPPY + 1; // 2
    public static final int WRITER_COMPRESSION_LZO = WRITER_COMPRESSION_GZIP + 1; // 3
    public static final int WRITER_COMPRESSION_BROTLI = WRITER_COMPRESSION_LZO + 1; // 4
    public static final int WRITER_COMPRESSION_LZ4 = WRITER_COMPRESSION_BROTLI + 1; // 5
    public static final int WRITER_COMPRESSION_ZSTD = WRITER_COMPRESSION_LZ4 + 1; // 6
    public static final int WRITER_COMPRESSION_LZ4_RAW = WRITER_COMPRESSION_ZSTD + 1; // 7
    private static final int[] CODEC_MAPPING = {
            WRITER_COMPRESSION_UNCOMPRESSED,
            WRITER_COMPRESSION_SNAPPY,
            WRITER_COMPRESSION_GZIP,
            WRITER_COMPRESSION_BROTLI,
            WRITER_COMPRESSION_ZSTD,
            WRITER_COMPRESSION_LZ4_RAW
    };
    public static final int ZSTD_MAX_COMPRESSION_LEVEL = 22;
    public static final int ZSTD_MIN_COMPRESSION_LEVEL = 1;
    private static final StringSink CODEC_NAMES = new StringSink(64);
    private static final IntObjHashMap<CharSequence> codecToNameMap = new IntObjHashMap<>(16);
    private static final LowerCaseCharSequenceIntHashMap nameToCodecMap = new LowerCaseCharSequenceIntHashMap(32);

    public static void addCodecNamesToException(SqlException e) {
        e.put(CODEC_NAMES);
    }

    public static int getCompressionCodec(CharSequence name) {
        return nameToCodecMap.get(name);
    }

    public static CharSequence getCompressionName(int codec) {
        return codecToNameMap.get(codec);
    }

    public static long packCompressionCodecLevel(int codec, long level) {
        if (codec < 0 || codec >= MAX_ENUM_INT) {
            throw new IllegalArgumentException("Invalid codec: " + codec + ", must be in range [0, " + MAX_ENUM_INT + ")");
        }
        return (level << 32) | CODEC_MAPPING[codec];
    }

    /**
     * Validates that the compression level is within the allowed range for the given codec.
     * Codecs that ignore the level (uncompressed, snappy, lz4_raw) accept any value silently.
     *
     * @param codec    one of the {@code COMPRESSION_*} constants
     * @param level    the user-supplied compression level
     * @param levelPos character position in the SQL text, used for error reporting
     */
    public static void validateCompressionLevel(int codec, int level, int levelPos) throws SqlException {
        switch (codec) {
            case COMPRESSION_UNCOMPRESSED, COMPRESSION_SNAPPY, COMPRESSION_LZ4_RAW -> {
                // these codecs ignore the level
            }
            case COMPRESSION_GZIP -> {
                if (level < GZIP_MIN_COMPRESSION_LEVEL || level > GZIP_MAX_COMPRESSION_LEVEL) {
                    throw SqlException.$(levelPos, "GZIP compression level must be between ")
                            .put(GZIP_MIN_COMPRESSION_LEVEL).put(" and ").put(GZIP_MAX_COMPRESSION_LEVEL);
                }
            }
            case COMPRESSION_BROTLI -> {
                if (level < BROTLI_MIN_COMPRESSION_LEVEL || level > BROTLI_MAX_COMPRESSION_LEVEL) {
                    throw SqlException.$(levelPos, "Brotli compression level must be between ")
                            .put(BROTLI_MIN_COMPRESSION_LEVEL).put(" and ").put(BROTLI_MAX_COMPRESSION_LEVEL);
                }
            }
            case COMPRESSION_ZSTD -> {
                if (level < ZSTD_MIN_COMPRESSION_LEVEL || level > ZSTD_MAX_COMPRESSION_LEVEL) {
                    throw SqlException.$(levelPos, "ZSTD compression level must be between ")
                            .put(ZSTD_MIN_COMPRESSION_LEVEL).put(" and ").put(ZSTD_MAX_COMPRESSION_LEVEL);
                }
            }
            default -> throw SqlException.$(levelPos, "unknown compression codec: ").put(codec);
        }
    }

    static {
        nameToCodecMap.put("uncompressed", COMPRESSION_UNCOMPRESSED);
        nameToCodecMap.put("zstd", COMPRESSION_ZSTD);
        nameToCodecMap.put("gzip", COMPRESSION_GZIP);
        nameToCodecMap.put("lz4_raw", COMPRESSION_LZ4_RAW);
        nameToCodecMap.put("snappy", COMPRESSION_SNAPPY);
        nameToCodecMap.put("brotli", COMPRESSION_BROTLI);
        nameToCodecMap.put("default", COMPRESSION_LZ4_RAW);

        codecToNameMap.put(COMPRESSION_UNCOMPRESSED, "uncompressed");
        codecToNameMap.put(COMPRESSION_ZSTD, "zstd");
        codecToNameMap.put(COMPRESSION_GZIP, "gzip");
        codecToNameMap.put(COMPRESSION_LZ4_RAW, "lz4_raw");
        codecToNameMap.put(COMPRESSION_SNAPPY, "snappy");
        codecToNameMap.put(COMPRESSION_BROTLI, "brotli");

        for (int i = 0, n = MAX_ENUM_INT; i < n; i++) {
            CODEC_NAMES.put(codecToNameMap.get(i));
            if (i + 1 != n) {
                CODEC_NAMES.put(", ");
            }
        }
    }
}
