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

package com.questdb;

import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalMetadataBuilder;
import com.questdb.model.Quote;
import com.questdb.store.UnstructuredFile;
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
        JournalMetadata m = b.build();
        m.write(hb);
        JournalMetadata metadata = new JournalMetadata(hb, null);
        hb.close();
        Assert.assertTrue(m.isCompatible(metadata, false));
    }
}
