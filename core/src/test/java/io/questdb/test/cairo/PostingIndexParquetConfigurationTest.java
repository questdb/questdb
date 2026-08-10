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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every test reaches the real {@code PropServerConfiguration} through
 * {@code CairoTestConfiguration extends CairoConfigurationWrapper}. A getter the
 * wrapper does not forward silently resolves to the {@code CairoConfiguration}
 * interface default instead, so setting the property has no effect and any test
 * built on it is vacuous. These tests pin the forwarding.
 */
public class PostingIndexParquetConfigurationTest extends AbstractCairoTest {

    @Test
    public void testParquetPartitionFormatOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                configuration.getPostingIndexParquetPartitionFormat()
        ));
    }

    @Test
    public void testParquetPayloadOverrideReachesWrappedConfiguration() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PAYLOAD, "row_per_key");
        assertMemoryLeak(() -> Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_KEY,
                configuration.getPostingIndexParquetPayload()
        ));
    }
}
