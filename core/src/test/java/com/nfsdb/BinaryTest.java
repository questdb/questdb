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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Rnd;
import com.nfsdb.model.Band;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BinaryTest extends AbstractTest {

    private static final Log LOGGER = LogFactory.getLog(BinaryTest.class);

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
        LOGGER.info().$("Appended ").$(count).$(" 10k blobs in ").$(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t)).$("ms.").$();
    }
}
