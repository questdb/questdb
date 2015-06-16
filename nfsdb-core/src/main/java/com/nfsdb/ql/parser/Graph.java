/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;

import java.util.ArrayDeque;

public class Graph {

    public static void main(String[] args) {
        Node seven = new Node("7");
        Node five = new Node("5");
        Node three = new Node("3");
        Node eleven = new Node("11");
        Node eight = new Node("8");
        Node two = new Node("2");
        Node nine = new Node("9");
        Node ten = new Node("10");
        seven.addEdge(eleven).addEdge(eight);
        five.addEdge(eleven);
        three.addEdge(eight).addEdge(ten);
        eleven.addEdge(two).addEdge(nine).addEdge(ten);
        eight.addEdge(nine).addEdge(ten);

        Node[] allNodes = {seven, five, three, eleven, eight, two, nine, ten};
        //ordered <- Empty list that will contain the sorted elements
        ObjList<Node> ordered = new ObjList<>();

        //stack <- Set of all nodes with no incoming edges
        ArrayDeque<Node> stack = new ArrayDeque<>();
        for (Node n : allNodes) {
            if (n.in == 0) {
                stack.addFirst(n);
            }
        }

        //while stack is non-empty do
        while (!stack.isEmpty()) {
            //remove a node n from stack
            Node n = stack.pollFirst();

            //insert n into ordered
            ordered.add(n);

            //for each node m with an edge e from n to m do
            for (int i = 0, k = n.out.size(); i < k; i++) {
                Node m = n.out.get(i);
                if ((--m.in) == 0) {
                    stack.addFirst(m);
                }
            }
            n.out.clear();
        }
        //Check to see if all edges are removed
        boolean cycle = false;
        for (Node n : allNodes) {
            if (n.in > 0) {
                cycle = true;
                break;
            }
        }
        if (cycle) {
            System.out.println("Cycle present, topological sort not possible");
        } else {
            System.out.println("Topological Sort: " + ordered);
        }
    }

    static class Node {
        public final String name;
        public final ObjHashSet<Node> out;
        public int in;

        public Node(String name) {
            this.name = name;
            in = 0;
            out = new ObjHashSet<>();
        }

        public Node addEdge(Node node) {
            out.add(node);
            node.in++;
            return this;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass()) && name.equals(((Node) o).name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}