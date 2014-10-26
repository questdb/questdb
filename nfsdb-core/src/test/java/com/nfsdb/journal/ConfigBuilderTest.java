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

import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import org.junit.Test;



public class ConfigBuilderTest {
    @Test
    public void testConfigWithoutClass() throws Exception {
        JournalConfigurationBuilder builder = new JournalConfigurationBuilder() {{
            $("test")
                    .$sym("x")
                    .$int("y")
                    .$str("z")
                    .$bin("b")
            ;
        }};

        JournalFactory factory = new JournalFactory(builder.build("."));

        JournalWriter w = factory.writer(new JournalKey<>("test"));
        System.out.println(w.getMetadata());
    }
}
