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

package io.questdb.test.griffin.model;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.str.AsciiCharSequence;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionNodeTest {

    @Test
    public void testDeepHashCodeConsistentWithCompareNodesExact() {
        // AsciiCharSequence does not override hashCode(), so it uses identity-based Object.hashCode().
        // This test verifies that deepHashCode uses content-based hashing for tokens,
        // consistent with compareNodesExact which uses Chars.equals for comparison.
        AsciiCharSequence token1 = new AsciiCharSequence().of(new Utf8String("test"));
        AsciiCharSequence token2 = new AsciiCharSequence().of(new Utf8String("test"));

        // Sanity check: tokens are different instances with same content
        Assert.assertNotSame(token1, token2);

        ExpressionNode node1 = ExpressionNode.FACTORY.newInstance();
        ExpressionNode node2 = ExpressionNode.FACTORY.newInstance();

        // Use CONSTANT type which uses case-sensitive Chars.equals in compareNodesExact
        node1.of(ExpressionNode.CONSTANT, token1, 0, 0);
        node2.of(ExpressionNode.CONSTANT, token2, 0, 0);

        // Nodes should be equal by content
        Assert.assertTrue(ExpressionNode.compareNodesExact(node1, node2));

        // Hash codes must be equal for equal nodes (hash/equality contract)
        Assert.assertEquals(ExpressionNode.deepHashCode(node1), ExpressionNode.deepHashCode(node2));
    }

    @Test
    public void testEmptyLiteral() {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance();
        node.of(ExpressionNode.LITERAL, "", 0, 0);
        Assert.assertEquals(ExpressionNode.LITERAL, node.type);
    }

}
