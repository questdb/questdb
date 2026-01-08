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

import io.questdb.cairo.BitmapIndexBwdReader;
import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolColumnIndexer;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fuzz test to verify that index() and rebuildNewIndex() produce semantically equivalent indexes.
 * The on-disk layout may differ, but the parsed contents should be identical.
 */
public class BitmapIndexRebuildFuzzTest extends AbstractCairoTest {

    private static final long COLUMN_NAME_TXN_NONE = -1L;

    /**
     * Represents the parsed contents of a bitmap index.
     * Used to compare indexes built via different methods.
     */
    public static class BitmapIndexContents {
        public final IntObjHashMap<LongList> mappings = new IntObjHashMap<>();
        public long maxValue;

        public void clear() {
            mappings.clear();
            maxValue = -1;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BitmapIndexContents)) {
                return false;
            }
            BitmapIndexContents other = (BitmapIndexContents) obj;
            if (maxValue != other.maxValue) {
                return false;
            }
            if (mappings.size() != other.mappings.size()) {
                return false;
            }
            for (int i = 0, n = mappings.size(); i < n; i++) {
                int key = mappings.getKeys()[i];
                if (key == mappings.getNoEntryKey()) {
                    continue;
                }
                LongList values = mappings.get(key);
                LongList otherValues = other.mappings.get(key);
                if (otherValues == null) {
                    return false;
                }
                if (values.size() != otherValues.size()) {
                    return false;
                }
                for (int j = 0, m = values.size(); j < m; j++) {
                    if (values.getQuick(j) != otherValues.getQuick(j)) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BitmapIndexContents{maxValue=").append(maxValue).append(", keys=[");
            int count = 0;
            for (int i = 0, n = mappings.size(); i < n; i++) {
                int key = mappings.getKeys()[i];
                if (key == mappings.getNoEntryKey()) {
                    continue;
                }
                if (count > 0) sb.append(", ");
                LongList values = mappings.get(key);
                sb.append(key).append("->").append(values.size()).append(" values");
                count++;
            }
            sb.append("]}");
            return sb.toString();
        }
    }

    /**
     * Creates an empty bitmap index.
     */
    private static void createIndex(CairoConfiguration configuration, Path path, CharSequence name, int valueBlockCapacity) {
        int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    MemoryMA mem = Vm.getSmallCMARWInstance(
                            ff,
                            BitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                BitmapIndexWriter.initKeyMemory(mem, Numbers.ceilPow2(valueBlockCapacity));
            }
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Parses a bitmap index into a BitmapIndexContents structure.
     */
    private static BitmapIndexContents parseIndex(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            int maxKey
    ) {
        BitmapIndexContents contents = new BitmapIndexContents();
        int plen = path.size();

        try (BitmapIndexBwdReader reader = new BitmapIndexBwdReader(
                configuration, path, name, COLUMN_NAME_TXN_NONE, -1, 0)) {

            // Read maxValue from the key file header directly
            path.trimTo(plen);
            contents.maxValue = readMaxValueFromHeader(configuration.getFilesFacade(), path, name);

            // Read all values for each key
            for (int key = 0; key <= maxKey; key++) {
                RowCursor cursor = reader.getCursor(false, key, 0, Long.MAX_VALUE);
                LongList values = new LongList();
                while (cursor.hasNext()) {
                    values.add(cursor.next());
                }
                // Reverse to get ascending order (BwdReader returns descending)
                if (values.size() > 0) {
                    LongList reversed = new LongList(values.size());
                    for (int i = values.size() - 1; i >= 0; i--) {
                        reversed.add(values.getQuick(i));
                    }
                    contents.mappings.put(key, reversed);
                }
            }
        } finally {
            path.trimTo(plen);
        }
        return contents;
    }

    private static long readMaxValueFromHeader(FilesFacade ff, Path path, CharSequence name) {
        int plen = path.size();
        try {
            BitmapIndexUtils.keyFileName(path, name, COLUMN_NAME_TXN_NONE);
            long fd = ff.openRO(path.$());
            if (fd == -1) {
                return -1;
            }
            try {
                // maxValue is at offset 37 in the header
                long maxValue = ff.readNonNegativeLong(fd, 37);
                return maxValue;
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Generates test data with an interesting distribution:
     * - Some keys occur very frequently (high frequency)
     * - Some keys occur rarely (low frequency)
     * - Some keys don't occur at all (sparse)
     *
     * Uses a power-law-like distribution to create realistic skew.
     */
    private static int[] generateTestData(Rnd rnd, int rowCount, int maxKey) {
        int[] data = new int[rowCount];

        // Create a biased distribution:
        // - 10% of keys get 70% of the occurrences (frequent keys)
        // - 30% of keys get 25% of the occurrences (medium keys)
        // - 60% of keys get 5% of the occurrences (rare keys) or don't occur at all

        for (int i = 0; i < rowCount; i++) {
            double r = rnd.nextDouble();
            int key;
            if (r < 0.70) {
                // 70% of rows: frequent keys (first 10% of key space)
                key = rnd.nextInt(Math.max(1, maxKey / 10));
            } else if (r < 0.95) {
                // 25% of rows: medium frequency keys (next 30% of key space)
                key = maxKey / 10 + rnd.nextInt(Math.max(1, (maxKey * 3) / 10));
            } else {
                // 5% of rows: rare keys (remaining 60% of key space)
                key = (maxKey * 4) / 10 + rnd.nextInt(Math.max(1, (maxKey * 6) / 10));
            }
            data[i] = Math.min(key, maxKey - 1);  // Ensure within bounds
        }
        return data;
    }

    /**
     * Creates test data file on disk.
     */
    private static void writeTestDataFile(FilesFacade ff, Path path, int[] data, int writerFileOpenOpts) {
        long fd = ff.openRW(path.$(), writerFileOpenOpts);
        Assert.assertTrue(fd > 0);
        try {
            long buffer = Unsafe.malloc(data.length * 4L, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < data.length; i++) {
                    Unsafe.getUnsafe().putInt(buffer + i * 4L, data[i]);
                }
                long written = ff.write(fd, buffer, data.length * 4L, 0);
                Assert.assertEquals(data.length * 4L, written);
            } finally {
                Unsafe.free(buffer, data.length * 4L, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    @Test
    public void testIndexAndRebuildNewIndexProduceSameResults() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            // Test parameters
            int[] rowCounts = {100, 1000, 10000, 50000};
            int[] maxKeys = {3, 10, 50, 200, 1000};

            for (int rowCount : rowCounts) {
                for (int maxKey : maxKeys) {
                    testWithParams(rnd, rowCount, maxKey);
                }
            }
        });
    }

    @Test
    public void testVerySkewedDistribution() throws Exception {
        assertMemoryLeak(() -> {
            // Test with extreme skew: 1 key has most values, others very rare
            Rnd rnd = new Rnd();
            int rowCount = 10000;
            int maxKey = 100;

            int[] data = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                double r = rnd.nextDouble();
                if (r < 0.90) {
                    // 90% of rows go to key 0
                    data[i] = 0;
                } else if (r < 0.95) {
                    // 5% go to key 1
                    data[i] = 1;
                } else {
                    // 5% spread across remaining keys
                    data[i] = 2 + rnd.nextInt(maxKey - 2);
                }
            }

            testWithData(data, maxKey, "very skewed");
        });
    }

    @Test
    public void testSparseKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Test with many sparse keys (most keys have 0 or 1 occurrence)
            Rnd rnd = new Rnd();
            int rowCount = 1000;
            int maxKey = 5000;  // Many more keys than rows

            int[] data = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                data[i] = rnd.nextInt(maxKey);
            }

            testWithData(data, maxKey, "sparse keys");
        });
    }

    @Test
    public void testAllSameKey() throws Exception {
        assertMemoryLeak(() -> {
            // Test with all rows having the same key
            int rowCount = 1000;
            int maxKey = 10;

            int[] data = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                data[i] = 5;  // All rows have key 5
            }

            testWithData(data, maxKey, "all same key");
        });
    }

    @Test
    public void testSequentialKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Test with sequential keys (each row has a different key)
            int rowCount = 500;
            int maxKey = 500;

            int[] data = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                data[i] = i;
            }

            testWithData(data, maxKey, "sequential keys");
        });
    }

    @Test
    public void testKeyZeroOnly() throws Exception {
        assertMemoryLeak(() -> {
            // Test with only key 0 (null key handling)
            int rowCount = 1000;
            int maxKey = 10;

            int[] data = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                data[i] = 0;
            }

            testWithData(data, maxKey, "key zero only");
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            // Test with single row
            int[] data = new int[]{5};
            testWithData(data, 10, "single row");
        });
    }

    @Test
    public void testTwoRows() throws Exception {
        assertMemoryLeak(() -> {
            // Test with two rows, same key
            int[] data = new int[]{3, 3};
            testWithData(data, 10, "two rows same key");

            // Test with two rows, different keys
            int[] data2 = new int[]{3, 7};
            testWithData(data2, 10, "two rows different keys");
        });
    }

    private void testWithParams(Rnd rnd, int rowCount, int maxKey) throws Exception {
        int[] data = generateTestData(rnd, rowCount, maxKey);
        testWithData(data, maxKey, "rowCount=" + rowCount + ", maxKey=" + maxKey);
    }

    private void testWithData(int[] data, int maxKey, String testDescription) throws Exception {
        FilesFacade ff = configuration.getFilesFacade();
        int rowCount = data.length;

        try (Path path = new Path()) {
            path.of(configuration.getDbRoot());
            int rootLen = path.size();

            // Create data file
            path.concat("test_data.d").$();
            writeTestDataFile(ff, path, data, configuration.getWriterFileOpenOpts());
            String dataFilePath = path.toString();

            // Build index using index() method
            path.trimTo(rootLen);
            BitmapIndexContents indexContents;
            try (SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration)) {
                path.concat("index_via_index");
                int plen = path.size();
                ff.mkdir(path.$(), configuration.getMkDirMode());
                path.slash();

                // Create index files
                createIndex(configuration, path.trimTo(plen).slash(), "test", 256);

                // Configure and run index()
                indexer.configureWriter(path.trimTo(plen).slash(), "test", COLUMN_NAME_TXN_NONE, 0);
                long dataFd = TableUtils.openRO(ff, Path.getThreadLocal(dataFilePath).$(), LOG);
                try {
                    indexer.index(ff, dataFd, 0, rowCount);
                } finally {
                    ff.close(dataFd);
                }
                indexer.sync(false);
                indexer.clear();

                // Parse the index
                indexContents = parseIndex(configuration, path.trimTo(plen).slash(), "test", maxKey);
            }

            // Build index using rebuildNewIndex() method
            path.trimTo(rootLen);
            BitmapIndexContents rebuildContents;
            try (SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration)) {
                path.concat("index_via_rebuild");
                int plen = path.size();
                ff.mkdir(path.$(), configuration.getMkDirMode());
                path.slash();

                // Create index files
                createIndex(configuration, path.trimTo(plen).slash(), "test", 256);

                // Configure and run rebuildNewIndex()
                indexer.configureWriter(path.trimTo(plen).slash(), "test", COLUMN_NAME_TXN_NONE, 0);
                long dataFd = TableUtils.openRO(ff, Path.getThreadLocal(dataFilePath).$(), LOG);
                try {
                    indexer.rebuildNewIndex(ff, dataFd, 0, rowCount);
                } finally {
                    ff.close(dataFd);
                }
                indexer.sync(false);
                indexer.clear();

                // Parse the index
                rebuildContents = parseIndex(configuration, path.trimTo(plen).slash(), "test", maxKey);
            }

            // Compare contents
            Assert.assertEquals(
                    "maxValue mismatch for " + testDescription +
                            "\nindex(): " + indexContents +
                            "\nrebuildNewIndex(): " + rebuildContents,
                    indexContents.maxValue,
                    rebuildContents.maxValue
            );

            // Compare all key mappings
            for (int key = 0; key <= maxKey; key++) {
                LongList indexValues = indexContents.mappings.get(key);
                LongList rebuildValues = rebuildContents.mappings.get(key);

                if (indexValues == null && rebuildValues == null) {
                    continue;
                }

                Assert.assertNotNull(
                        "Key " + key + " missing in rebuildNewIndex() result for " + testDescription,
                        rebuildValues
                );
                Assert.assertNotNull(
                        "Key " + key + " missing in index() result for " + testDescription,
                        indexValues
                );

                Assert.assertEquals(
                        "Value count mismatch for key " + key + " in " + testDescription,
                        indexValues.size(),
                        rebuildValues.size()
                );

                for (int i = 0; i < indexValues.size(); i++) {
                    Assert.assertEquals(
                            "Value mismatch for key " + key + " at position " + i + " in " + testDescription,
                            indexValues.getQuick(i),
                            rebuildValues.getQuick(i)
                    );
                }
            }

            // Cleanup
            path.trimTo(rootLen).concat("test_data.d").$();
            ff.remove(path.$());
        }
    }
}
