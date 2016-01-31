/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.JournalMetadataBuilder;
import com.nfsdb.model.Quote;
import com.nfsdb.store.UnstructuredFile;
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

        UnstructuredFile hb = new UnstructuredFile(temp.newFile(), 10, JournalMode.APPEND);
        JournalMetadata m = (JournalMetadata) b.build();
        m.write(hb);
        JournalMetadata metadata = new JournalMetadata(hb);
        hb.close();
        Assert.assertTrue(m.isCompatible(metadata, false));
    }
}
