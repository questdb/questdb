/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.model.Band;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BinaryTest extends AbstractTest {

    private static final Logger LOGGER = Logger.getLogger(BinaryTest.class);

    @Test
    public void testBinaryAppend() throws Exception {
        JournalWriter<Band> writer = factory.writer(Band.class);

        Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());
        List<byte[]> bytes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            bytes.add(r.nextBytes((3 - i) * 1024));
        }

        writer.append(new Band().setName("Supertramp").setType("jazz").setImage(bytes.get(0)));
        writer.append(new Band().setName("TinieTempah").setType("rap").setImage(bytes.get(1)));
        writer.append(new Band().setName("Rihanna").setType("pop").setImage(bytes.get(2)));
        writer.commit();

        int count = 0;
        for (Band b : writer) {
            Assert.assertArrayEquals(bytes.get(count), b.getImage().array());
            count++;
        }
    }

    @Test
    public void testBinaryPerformance() throws Exception {

        JournalWriter<Band> writer = factory.bulkWriter(Band.class);
        final int count = 20000;
        Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());

        byte[] bytes = r.nextBytes(10240);
        String[] types = new String[]{"jazz", "rap", "pop", "rock", "soul"};
        String[] bands = new String[1200];
        for (int i = 0; i < bands.length; i++) {
            bands[i] = r.nextString(10);
        }

        long t = System.nanoTime();
        Band band = new Band();
        for (int i = 0; i < count; i++) {
            band.setName(bands[Math.abs(r.nextInt() % bands.length)]);
            band.setType(types[Math.abs(r.nextInt() % types.length)]);
            band.setImage(bytes);
            writer.append(band);
        }
        writer.commit();
        LOGGER.info("Appended " + count + " 10k blobs in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
    }
}
