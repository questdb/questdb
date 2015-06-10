/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.ql.model;

import com.nfsdb.collections.ObjList;

public class ExprNode {

    public final String token;
    public final int precedence;
    public final int position;
    public ExprNode lhs;
    public ExprNode rhs;
    public NodeType type;
    public int paramCount;
    public ObjList<ExprNode> args;
    public IntrinsicValue intrinsicValue = IntrinsicValue.UNDEFINED;

    public ExprNode(NodeType type, String token, int precedence, int position) {
        this.type = type;
        this.precedence = precedence;
        this.token = token;
        this.position = position;
    }

    @Override
    public String toString() {
        return "ExprNode{" +
                "token='" + token + '\'' +
                ", precedence=" + precedence +
                ", lhs=" + lhs +
                ", rhs=" + rhs +
                ", type=" + type +
                ", paramCount=" + paramCount +
                ", args=" + args +
                ", position=" + position +
                '}';
    }

    public enum NodeType {
        OPERATION, CONSTANT, LITERAL, FUNCTION, CONTROL, SET_OPERATION
    }
}
