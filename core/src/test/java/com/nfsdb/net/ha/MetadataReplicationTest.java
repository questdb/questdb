/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.net.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.comsumer.HugeBufferConsumer;
import com.nfsdb.net.ha.producer.HugeBufferProducer;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MetadataReplicationTest extends AbstractTest {
    @Test
    public void testReplication() throws Exception {

        JournalWriter w = factory.writer(Quote.class);

        MockByteChannel channel = new MockByteChannel();
        HugeBufferProducer p = new HugeBufferProducer(new File(w.getMetadata().getLocation(), JournalConfiguration.FILE_NAME));
        HugeBufferConsumer c = new HugeBufferConsumer(new File(w.getMetadata().getLocation(), "_remote"));
        p.write(channel);
        c.read(channel);

        JournalWriter w2 = factory.writer(
                new JournalStructure(
                        new JournalMetadata(c.getHb())
                ).location("xyz")
        );

        Assert.assertTrue(w.getMetadata().isCompatible(w2.getMetadata(), false));

        w2.close();
        w.close();

        p.free();
        c.free();
    }
}
