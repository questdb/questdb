/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.CompositeDimensionTransform;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct unit coverage for {@link CompositeDimensionTransform#truncatedPrefix(CharSequence, int,
 * StringSink)}'s passthrough edge case: it is documented to return {@code value} completely
 * unchanged (not merely equal-valued) whenever {@code value} is {@code null} or already no longer
 * than {@code n}, and only materializes into {@code sink} when a real truncation is needed. This had
 * zero direct coverage -- {@link CompositeDictPersistenceTest} and {@link CompositeDictionariesTest}
 * only exercise it indirectly through {@code TableWriter.internDimensionValue}'s {@code TRUNCATE}
 * branch, and always with a value longer than {@code n}.
 */
public class CompositeDimensionTransformTest {

    @Test
    public void testTruncatedPrefixPassthrough() {
        StringSink sink = new StringSink();

        // null value -> null, unchanged
        Assert.assertNull(CompositeDimensionTransform.truncatedPrefix(null, 3, sink));

        // length 0 <= n -> unchanged
        TestUtils.assertEquals("", CompositeDimensionTransform.truncatedPrefix("", 3, sink));

        // length 2 <= n -> unchanged
        TestUtils.assertEquals("ab", CompositeDimensionTransform.truncatedPrefix("ab", 3, sink));

        // length 3 == n (boundary) -> unchanged, not routed through sink
        TestUtils.assertEquals("abc", CompositeDimensionTransform.truncatedPrefix("abc", 3, sink));

        // length 4 > n -> truncated to the first n chars via sink
        TestUtils.assertEquals("abc", CompositeDimensionTransform.truncatedPrefix("abcd", 3, sink));
    }
}
