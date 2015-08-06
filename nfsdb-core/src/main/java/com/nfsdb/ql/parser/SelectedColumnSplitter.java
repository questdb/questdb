/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.parser;

import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.ql.model.ExprNode;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

public class SelectedColumnSplitter {
    private final Set<String> aggregatorFunctions = new HashSet<>();
    private final ObjObjHashMap<String, ExprNode> innerTrees = new ObjObjHashMap<>();
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 16);
    private int sequence;

    public SelectedColumnSplitter() {
        aggregatorFunctions.add("sum");
        aggregatorFunctions.add("count");
    }

    public static void main(String[] args) throws ParserException {
        ExprParser parser = new ExprParser();
        AstBuilder builder = new AstBuilder();
        parser.parseExpr("a+sum(b)*count()", builder);


        SelectedColumnSplitter splitter = new SelectedColumnSplitter();
        splitter.detachAggregates(builder.root());

        System.out.println("ok");
    }

    private ExprNode detachAggregates(ExprNode node) {

        // pre-order iterative tree traversal
        // see: http://en.wikipedia.org/wiki/Tree_traversal

        ArrayDeque<ExprNode> stack = new ArrayDeque<>();
        ExprNode root = node;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {

                if (node.rhs != null) {
                    ExprNode n = replaceIfAggregate(node.rhs);
                    if (node.rhs == n) {
                        stack.push(node.rhs);
                    } else {
                        node.rhs = n;
                    }
                }

                ExprNode n = replaceIfAggregate(node.lhs);
                if (n == node.lhs) {
                    node = node.lhs;
                } else {
                    node.lhs = n;
                    node = null;
                }
            } else {
                node = stack.poll();
            }
        }
        return root;
    }

    private ExprNode replaceIfAggregate(ExprNode node) {
        if (node != null && aggregatorFunctions.contains(node.token)) {
            String token = "col" + sequence++;
            innerTrees.put(token, node);
            return exprNodePool.next().init(ExprNode.NodeType.LITERAL, token, 0, 0);
        }
        return node;
    }
}
