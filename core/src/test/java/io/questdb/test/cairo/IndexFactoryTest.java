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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class IndexFactoryTest extends AbstractCairoTest {

    @Test
    public void testKeyFileNameBitmap() {
        try (Path path = new Path().put("/tmp/test/")) {
            IndexFactory.keyFileName(IndexType.BITMAP, path, "col", 0);
            Assert.assertTrue(path.toString().contains(".k"));
        }
    }

    @Test
    public void testKeyFileNamePosting() {
        try (Path path = new Path().put("/tmp/test/")) {
            IndexFactory.keyFileName(IndexType.POSTING, path, "col", 0);
            Assert.assertTrue(path.toString().contains(".pk"));
        }
    }

    @Test
    public void testKeyFileNamePostingDelta() {
        try (Path path = new Path().put("/tmp/test/")) {
            IndexFactory.keyFileName(IndexType.POSTING_DELTA, path, "col", 0);
            Assert.assertTrue(path.toString().contains(".pk"));
        }
    }

    @Test
    public void testKeyFileNameUnsupportedThrows() {
        try (Path path = new Path().put("/tmp/test/")) {
            try {
                IndexFactory.keyFileName((byte) 99, path, "col", 0);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("unsupported index type"));
            }
        }
    }

    @Test
    public void testValueFileNameUnsupportedThrows() {
        try (Path path = new Path().put("/tmp/test/")) {
            try {
                IndexFactory.valueFileName((byte) 99, path, "col", 0);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("unsupported index type"));
            }
        }
    }

    @Test
    public void testInitKeyMemoryNoneThrows() {
        try {
            IndexFactory.initKeyMemory(IndexType.NONE, null, 256);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("cannot initialize key memory"));
        }
    }

    @Test
    public void testInitKeyMemoryUnsupportedThrows() {
        try {
            IndexFactory.initKeyMemory((byte) 99, null, 256);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("unsupported index type"));
        }
    }

    @Test
    public void testCreateReaderNoneThrows() {
        try (Path path = new Path().put("/tmp/test/")) {
            try {
                IndexFactory.createReader(IndexType.NONE, 1, configuration, path, "col", 0, 0, 0);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("cannot create reader"));
            }
        }
    }

    @Test
    public void testCreateReaderUnsupportedThrows() {
        try (Path path = new Path().put("/tmp/test/")) {
            try {
                IndexFactory.createReader((byte) 99, 1, configuration, path, "col", 0, 0, 0);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("unsupported index type"));
            }
        }
    }

    @Test
    public void testCreateWriterNoneThrows() {
        try {
            IndexFactory.createWriter(IndexType.NONE, configuration);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("cannot create writer"));
        }
    }

    @Test
    public void testCreateWriterUnsupportedThrows() {
        try {
            IndexFactory.createWriter((byte) 99, configuration);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("unsupported index type"));
        }
    }

    @Test
    public void testCreateWriterPostingDelta() {
        // POSTING_DELTA creates a PostingIndexWriter — verify it opens without error
        try (var writer = IndexFactory.createWriter(IndexType.POSTING_DELTA, configuration)) {
            Assert.assertNotNull(writer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
