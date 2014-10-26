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

import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import org.junit.Test;

public class WriterDoubleCloseTest extends AbstractTest {
    @Test
    public void testDoubleClose() throws Exception {
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 100);
            w.close();
        }
    }
}
