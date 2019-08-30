/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.std.str;

import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class DirectCharSinkTest {
    @Test
    public void testResize() {
        final String expected = "string 0\n" +
                "string 1\n" +
                "string 2\n" +
                "string 3\n" +
                "string 4\n" +
                "string 5\n" +
                "string 6\n" +
                "string 7\n" +
                "string 8\n" +
                "string 9\n" +
                "string 10\n" +
                "string 11\n" +
                "string 12\n" +
                "string 13\n" +
                "string 14\n" +
                "string 15\n" +
                "string 16\n" +
                "string 17\n" +
                "string 18\n" +
                "string 19\n" +
                "string 20\n" +
                "string 21\n" +
                "string 22\n" +
                "string 23\n" +
                "string 24\n" +
                "string 25\n" +
                "string 26\n" +
                "string 27\n" +
                "string 28\n" +
                "string 29\n";

        try (DirectCharSink sink = new DirectCharSink(16)) {
            for (int i = 0; i < 30; i++) {
                sink.put("string ").put(i).put('\n');
            }
            TestUtils.assertEquals(expected, sink);
            sink.clear();
            for (int i = 0; i < 30; i++) {
                sink.put("string ").put(i).put('\n');
            }
            TestUtils.assertEquals(expected, sink);
        }
    }
}