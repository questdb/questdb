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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CyclicalAstTest extends AbstractCairoTest {

    @Test
    public void testTraverseExit() throws SqlException {
        PostOrderTreeTraversalAlgo algo = new PostOrderTreeTraversalAlgo();

        ExpressionNode parent = ExpressionNode.FACTORY.newInstance();
        ExpressionNode lhs = ExpressionNode.FACTORY.newInstance();
        lhs.paramCount = 0;
        parent.token = "test";
        parent.paramCount = 2;
        parent.lhs = ExpressionNode.FACTORY.newInstance();
        parent.rhs = parent;
        try {
            algo.traverse(parent, node -> {
            });
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains("detected a recursive expression AST", e.getFlyweightMessage());
        }
    }
}
