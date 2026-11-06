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

import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PostOrderTreeTraversalAlgoTest {
    @Test
    public void testDescendPrunesLeafAndSubtree() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode leaf = node("leaf");
        final ExpressionNode branch = binary("branch", node("a"), node("b"));
        final ExpressionNode root = ternary("root", leaf, branch, node("keep"));
        traversal.traverse(root, new RecordingVisitor(sink) {
            @Override
            public boolean descend(ExpressionNode node) throws SqlException {
                super.descend(node);
                return node != leaf && node != branch;
            }
        });
        TestUtils.assertEquals("d:root d:leaf d:branch d:keep v:keep v:root ", sink);
    }

    @Test
    public void testDescendRewritesNodesBeforeLeafCheck() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode left = binary("left", node("discarded1"), node("discarded2"));
        final ExpressionNode right = node("right");
        traversal.traverse(binary("root", left, right), new RecordingVisitor(sink) {
            @Override
            public boolean descend(ExpressionNode node) throws SqlException {
                super.descend(node);
                if (node == right) {
                    node.paramCount = 2;
                    node.lhs = node("newLeft");
                    node.rhs = node("newRight");
                } else if (node == left) {
                    node.paramCount = 0;
                    node.lhs = null;
                    node.rhs = null;
                }
                return true;
            }
        });
        TestUtils.assertEquals("d:root d:right d:newRight v:newRight d:newLeft v:newLeft v:right d:left v:left v:root ", sink);
    }

    @Test
    public void testNestedTraversalFromDescendAndLeafVisit() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode right = node("right");
        final ExpressionNode innerLeaf = node("innerLeaf");
        traversal.traverse(binary("root", node("left"), right), new RecordingVisitor(sink) {
            @Override
            public boolean descend(ExpressionNode node) throws SqlException {
                super.descend(node);
                if (node == right) {
                    traversal.traverse(binary("inner", node("a"), node("b")), this);
                }
                return true;
            }

            @Override
            public void visit(ExpressionNode node) throws SqlException {
                super.visit(node);
                if (node == right) {
                    traversal.traverse(innerLeaf, this);
                } else if (node == innerLeaf) {
                    traversal.traverse(binary("deep", node("c"), node("d")), this);
                }
            }
        });
        TestUtils.assertEquals("d:root d:right d:inner d:b v:b d:a v:a v:inner v:right d:innerLeaf v:innerLeaf "
                + "d:deep d:d v:d d:c v:c v:deep d:left v:left v:root ", sink);
    }

    @Test
    public void testNestedVisitFailureRestoresParentTraversal() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode right = node("right");
        final ExpressionNode failure = node("failure");
        traversal.traverse(binary("root", node("left"), right), new RecordingVisitor(sink) {
            @Override
            public void visit(ExpressionNode node) throws SqlException {
                super.visit(node);
                if (node == failure) {
                    throw SqlException.$(7, "visit failed");
                }
                if (node == right) {
                    try {
                        traversal.traverse(binary("inner", node("unvisited"), failure), this);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(7, e.getPosition());
                        TestUtils.assertEquals("visit failed", e.getFlyweightMessage());
                        sink.put("caught ");
                    }
                }
            }
        });
        traversal.traverse(node("next"), new RecordingVisitor(sink));
        TestUtils.assertEquals("d:root d:right v:right d:inner d:failure v:failure caught d:left v:left v:root d:next v:next ", sink);
    }

    @Test
    public void testNullRootIsIgnored() throws SqlException {
        new PostOrderTreeTraversalAlgo().traverse(null, node -> Assert.fail());
    }

    @Test
    public void testTraversalOrder() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode unary = node("unary");
        unary.paramCount = 1;
        unary.rhs = node("d");
        final ExpressionNode root = ternary("root", node("a"), binary("binary", node("b"), node("c")), unary);
        traversal.traverse(root, new RecordingVisitor(sink));
        TestUtils.assertEquals("d:root d:a v:a d:binary d:c v:c d:b v:b v:binary d:unary d:d v:d v:unary v:root ", sink);
    }

    @Test
    public void testVisitFailureAllowsReuse() throws SqlException {
        final PostOrderTreeTraversalAlgo traversal = new PostOrderTreeTraversalAlgo();
        final StringSink sink = new StringSink();
        final ExpressionNode failure = node("failure");
        try {
            traversal.traverse(binary("root", node("unvisited"), failure), node -> {
                throw new IllegalStateException("visit failed");
            });
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals("visit failed", e.getMessage());
        }
        traversal.traverse(binary("next", node("a"), node("b")), new RecordingVisitor(sink));
        TestUtils.assertEquals("d:next d:b v:b d:a v:a v:next ", sink);
    }

    private static ExpressionNode binary(String token, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = node(token);
        node.paramCount = 2;
        node.lhs = left;
        node.rhs = right;
        return node;
    }

    private static ExpressionNode node(String token) {
        return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, token, 0, 0);
    }

    private static ExpressionNode ternary(String token, ExpressionNode first, ExpressionNode second, ExpressionNode third) {
        final ExpressionNode node = node(token);
        node.paramCount = 3;
        node.args.add(first);
        node.args.add(second);
        node.args.add(third);
        return node;
    }

    private static class RecordingVisitor implements PostOrderTreeTraversalAlgo.Visitor {
        private final StringSink sink;

        private RecordingVisitor(StringSink sink) {
            this.sink = sink;
        }

        @Override
        public boolean descend(ExpressionNode node) throws SqlException {
            sink.put("d:").put(node.token).put(' ');
            return true;
        }

        @Override
        public void visit(ExpressionNode node) throws SqlException {
            sink.put("v:").put(node.token).put(' ');
        }
    }
}
