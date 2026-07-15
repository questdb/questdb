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

package io.questdb.test.griffin;

import io.questdb.cairo.PartitionDimension;
import io.questdb.griffin.PartitionTransform;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class PartitionTransformTest extends AbstractCairoTest {

    // resolver maps "exchange"->1, "symbol"->2, anything else -> throws "not a SYMBOL column"
    private static final Function<CharSequence, Integer> RES = name -> {
        if (Chars.equalsIgnoreCase(name, "exchange")) {
            return 1;
        }
        if (Chars.equalsIgnoreCase(name, "symbol")) {
            return 2;
        }
        throw new RuntimeException("not a SYMBOL column: " + name);
    };

    @Test
    public void testIdentity() throws Exception {
        PartitionDimension d = PartitionTransform.resolve(lit("exchange"), RES);
        Assert.assertEquals(PartitionDimension.KIND_IDENTITY, d.getKind());
        Assert.assertEquals(1, d.getColumnIndex());
        Assert.assertEquals("exchange", d.getAlias());
    }

    @Test
    public void testHash() throws Exception {
        PartitionDimension d = PartitionTransform.resolve(fn("hash", lit("symbol"), num(32)), RES);
        Assert.assertEquals(PartitionDimension.KIND_HASH, d.getKind());
        Assert.assertEquals(2, d.getColumnIndex());
        Assert.assertEquals(32, d.getParam());
        Assert.assertEquals("symbol_hash", d.getAlias());
    }

    @Test
    public void testTruncatePrefix() throws Exception {
        PartitionDimension d = PartitionTransform.resolve(fn("truncate", lit("symbol"), num(3)), RES);
        Assert.assertEquals(PartitionDimension.KIND_TRUNCATE, d.getKind());
        Assert.assertEquals(3, d.getParam());
        Assert.assertEquals("symbol_trunc", d.getAlias());
    }

    @Test
    public void testHashOnNonSymbolThrows() {
        try {
            PartitionTransform.resolve(fn("hash", lit("price"), num(4)), RES);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "SYMBOL");
        }
    }

    @Test
    public void testHashRequiresPositiveN() {
        try {
            PartitionTransform.resolve(fn("hash", lit("symbol"), num(0)), RES);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bucket count");
        }
    }

    // Builds a FUNCTION node the same way the real expression parser does
    // (ExpressionTreeBuilder.onNode): paramCount == 1 -> rhs only, lhs null;
    // paramCount == 2 -> lhs = first arg, rhs = second arg.
    private static ExpressionNode fn(String name, ExpressionNode... args) {
        ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, name, 0, 0);
        node.paramCount = args.length;
        switch (args.length) {
            case 1:
                node.rhs = args[0];
                break;
            case 2:
                node.lhs = args[0];
                node.rhs = args[1];
                break;
            default:
                throw new IllegalArgumentException("fn() helper only supports 1 or 2 args");
        }
        return node;
    }

    private static ExpressionNode lit(String name) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, name, 0, 0);
    }

    private static ExpressionNode num(int value) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, Integer.toString(value), 0, 0);
    }
}
