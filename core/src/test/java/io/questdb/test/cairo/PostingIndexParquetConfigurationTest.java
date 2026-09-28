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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every test reaches the real {@code PropServerConfiguration} through
 * {@code CairoTestConfiguration extends CairoConfigurationWrapper}. A getter the
 * wrapper does not forward silently resolves to the {@code CairoConfiguration}
 * interface default instead, so setting the property has no effect and any test
 * built on it is vacuous. This test pins the forwarding.
 * <p>
 * It covered a second property, {@code cairo.posting.index.parquet.payload}, until that
 * property was removed for having no production consumer. Reintroduce both together when
 * the row-per-key payload arm gains a writer.
 */
public class PostingIndexParquetConfigurationTest extends AbstractCairoTest {

    @Test
    public void testParquetDataPageSizeFollowsACompressingCodec() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_COMPRESSION_CODEC, "LZ4_RAW");
        assertMemoryLeak(() -> Assert.assertEquals(16 * 1024, configuration.getPostingIndexParquetDataPageSize()));
    }

    @Test
    public void testParquetDataPageSizeOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_DATA_PAGE_SIZE, 32 * 1024);
        assertMemoryLeak(() -> Assert.assertEquals(32 * 1024, configuration.getPostingIndexParquetDataPageSize()));
    }

    @Test
    public void testParquetDataPageSizeDefaultFollowsTheCodec() throws Exception {
        // Uncompressed pages are never decompressed, so the only per-page cost
        // left is walking a thrift header to find the right one, and large
        // pages minimise that. Pairing UNCOMPRESSED with the compressing
        // codec's 16 KiB costs most of the win, so the default follows.
        assertMemoryLeak(() -> Assert.assertEquals(2 * 1024 * 1024, configuration.getPostingIndexParquetDataPageSize()));
    }

    @Test
    public void testParquetDataPageSizeExplicitOverridesTheCodecDefault() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_DATA_PAGE_SIZE, 8 * 1024);
        assertMemoryLeak(() -> Assert.assertEquals(8 * 1024, configuration.getPostingIndexParquetDataPageSize()));
    }

    @Test
    public void testParquetMaxKeysPerRowGroupOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_MAX_KEYS_PER_ROW_GROUP, 8);
        assertMemoryLeak(() -> Assert.assertEquals(8, configuration.getPostingIndexParquetMaxKeysPerRowGroup()));
    }

    @Test
    public void testParquetCompressionCodecOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_COMPRESSION_CODEC, "LZ4_RAW");
        assertMemoryLeak(() -> Assert.assertEquals(
                ParquetCompression.COMPRESSION_LZ4_RAW,
                configuration.getPostingIndexParquetCompressionCodec()
        ));
    }

    @Test
    public void testParquetPartitionFormatOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                configuration.getPostingIndexParquetPartitionFormat()
        ));
    }
}
