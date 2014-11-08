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

import com.nfsdb.journal.factory.configuration.JournalStructure;
import com.nfsdb.journal.test.tools.AbstractTest;
import org.junit.Test;

public class GenericInteropTest extends AbstractTest {
    @Test
    public void testGenericAll() throws Exception {

        JournalWriter writer = factory.writer(new JournalStructure("test") {{
            $sym("sym").index();
            $date("created");
            $double("bid");
            $double("ask");
            $int("bidSize");
            $int("askSize");
            $int("id").index();
            $str("status");
            $str("user");
            $str("rateId").index();
            $bool("active");
            $str("nullable");
            $long("ticks");
            $short("modulo");
        }});

        JournalEntryWriter w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, System.currentTimeMillis());
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 1);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.append();

        writer.commit();

    }
}
