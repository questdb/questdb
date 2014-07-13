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

import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.experimental.FilteredCursor;
import com.nfsdb.journal.index.experimental.IDSearch;
import com.nfsdb.journal.index.experimental.filter.IntEqualsFilter;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;

public class CursorTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory("/nfsdb-cursor-test.xml", null);

    @Test
    public void testAnd() throws Exception {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 3000000);

        TestUtils.generateQuoteDataRandomMode(w, 3000000);

        Journal<Quote> r = factory.reader(Quote.class);

        final IDSearch<Quote> search = new IDSearch<>();
        final String searchString = "Fast trading150";

        long t = 0;
        for (int i = -1000; i < 1000; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            LongArrayList result = r.iteratePartitionsDesc(new AbstractResultSetBuilder<Quote, LongArrayList>() {


                @Override
                public void read(long lo, long hi) throws JournalException {
                    search.createState(partition, "mode");

                    IntEqualsFilter filter = new IntEqualsFilter();
                    filter.setColumn(partition.getAbstractColumn(partition.getJournal().getMetadata().getColumnIndex("sym")));
                    filter.setSearchTerm(partition.getJournal().getSymbolTable("sym").getQuick("BP.L"));

                    FilteredCursor c = new FilteredCursor();
                    c.configure(search.exec(searchString), filter);

//                    Cursor cursor = index.cachedCursor(key);
//                    AndCursor cc = new AndCursor(cursor, c);
                    while (c.hasNext()) {
                        result.add(c.next());
                    }
                }

                @Override
                public LongArrayList getResult() {
                    return result;
                }
            });
//            System.out.println(result.size());
        }

        System.out.println((System.nanoTime() - t) / 1000);
    }
}
