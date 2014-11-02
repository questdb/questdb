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

import com.nfsdb.journal.column.HugeBuffer;
import com.nfsdb.journal.factory.configuration.JournalMetadataBuilder;
import com.nfsdb.journal.factory.configuration.JournalMetadataImpl;
import com.nfsdb.journal.model.Quote;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JournalMetadataTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testMetadataWrite() throws Exception {
        JournalMetadataBuilder<Quote> b = new JournalMetadataBuilder<>(Quote.class);

        HugeBuffer hb = new HugeBuffer(temp.newFile(), 10, JournalMode.APPEND);
        JournalMetadataImpl m = (JournalMetadataImpl) b.build();
        m.write(hb);
        JournalMetadataImpl metadata = new JournalMetadataImpl(hb);
        hb.close();
        Assert.assertEquals(m, metadata);
    }
}
