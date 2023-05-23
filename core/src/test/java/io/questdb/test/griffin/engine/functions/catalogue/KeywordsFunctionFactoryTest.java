/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.griffin.engine.functions.catalogue.Constants.KEYWORDS;

public class KeywordsFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testSelectKeywords() throws Exception {
        CharSequence[] keywords = KEYWORDS.clone();
        Arrays.sort(keywords);
        String expected = "keyword\n" + String.join("\n", keywords) + '\n';
        assertSql("select keyword from keywords() order by keyword asc", expected);
    }

    @Test
    public void testSelectKeywordsWithFilter() throws Exception {
        assertSql("keywords() where keyword = 'add'", "keyword\nadd\n");
    }
}
