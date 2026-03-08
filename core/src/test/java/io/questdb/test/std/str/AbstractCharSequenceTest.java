/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std.str;

import io.questdb.std.str.AbstractCharSequence;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AbstractCharSequenceTest {

    @Test
    public void testSubsquenece_theWholeRangeReturnsTheSameObject() {
        final CharSequence cs = new AbstractCharSequence() {
            private static final String str = "abc";

            @Override
            public char charAt(int index) {
                return str.charAt(index);
            }

            @Override
            public int length() {
                return str.length();
            }
        };
        CharSequence sub = cs.subSequence(0, cs.length());
        assertSame(cs, sub);
        assertEquals("abc", sub.toString());
    }
}
