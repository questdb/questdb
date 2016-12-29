/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Rnd;
import com.questdb.model.Band;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BinaryTest extends AbstractTest {

    private static final Log LOGGER = LogFactory.getLog(BinaryTest.class);

    @Test
    public void testBinaryAppend() throws Exception {
        try (JournalWriter<Band> writer = getWriterFactory().writer(Band.class)) {

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
    }

    @Test
    public void testBinaryPerformance() throws Exception {

        try (JournalWriter<Band> writer = getWriterFactory().writer(Band.class)) {
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
            LOGGER.info().$("Appended ").$(count).$(" 10k blobs in ").$(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t)).$("ms.").$();
        }
    }
}
