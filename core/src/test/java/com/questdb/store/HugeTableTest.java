/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.store;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Rnd;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import com.questdb.test.tools.FactoryContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HugeTableTest {

    private static final Log LOG = LogFactory.getLog(HugeTableTest.class);

    @ClassRule
    public static FactoryContainer factoryContainer = new FactoryContainer(new JournalConfigurationBuilder() {{
        $(Name.class).recordCountHint(15000000).txCountHint(1)
                .$sym("name").valueCountHint(15000000).index().noCache()
        ;
    }});

    @After
    public void tearDown() throws Exception {
        Assert.assertEquals(0, getFactory().getBusyReaderCount());
        Assert.assertEquals(0, getFactory().getBusyWriterCount());
    }

    @Test
    public void testLargeSymbolTable() throws Exception {
        try (JournalWriter<Name> w = getFactory().writer(Name.class, "name")) {
            Name name = new Name();
            Rnd rnd = new Rnd();

            long t = 0;
            for (int i = -500000; i < 2000000; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                name.name = rnd.nextString(10);
                w.append(name);
            }
            w.commit();
            LOG.info().$("Appended 2M symbols in ").$(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t)).$("ms").$();
        }
    }

    private static Factory getFactory() {
        return factoryContainer.getFactory();
    }

    public static class Name {
        String name;
    }
}
