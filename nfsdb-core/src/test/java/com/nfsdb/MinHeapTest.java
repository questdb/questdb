package com.nfsdb;

import com.nfsdb.collections.MinHeap;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class MinHeapTest extends AbstractTest {
    private static final int totalKeys = 10;
    private static final int totalValues = 100;
    private File indexFile;

    @Before
    public void setup() {
        indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
    }

    @Test
    public void testIndexSort() throws Exception {
        final int nStreams = 16;
        Rnd rnd = new Rnd();
        int totalLen = 0;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
            for (int i = 0; i < nStreams; i++) {
                long values[] = new long[rnd.nextPositiveInt() % 1000];
                totalLen += values.length;

                for (int j = 0; j < values.length; j++) {
                    values[j] = rnd.nextPositiveLong() % 100;
                }

                Arrays.sort(values);

                for (int j = 0; j < values.length; j++) {
                    index.add(i, values[j]);
                }
                index.commit();
            }


            long expected[] = new long[totalLen];
            int p = 0;

            for (int i = 0; i < nStreams; i++) {
                IndexCursor c = index.fwdCursor(i);
                while (c.hasNext()) {
                    expected[p++] = c.next();
                }
            }

            Arrays.sort(expected);

            MinHeap heap = new MinHeap(nStreams);
            IndexCursor cursors[] = new IndexCursor[nStreams];

            for (int i = 0; i < nStreams; i++) {
                cursors[i] = index.newFwdCursor(i);

                if (cursors[i].hasNext()) {
                    heap.add(i, cursors[i].next());
                }
            }

            p = 0;
            while (heap.hasNext()) {
                int idx = heap.popIndex();
                long v;
                if (cursors[idx].hasNext()) {
                    v = heap.popAndReplace(idx, cursors[idx].next());
                } else {
                    v = heap.popValue();
                }
                Assert.assertEquals(expected[p++], v);
            }
        }
    }
}
