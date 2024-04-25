/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin;

import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.ObjList;

public class OperatorRegistry {
    public final OperatorExpression unaryMinus;
    public final OperatorExpression unaryComplement;
    public final OperatorExpression setOperationNegation;
    public final OperatorExpression dot;
    public final ObjList<OperatorExpression> operators;
    public final LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression> map;

    public OperatorRegistry(ObjList<OperatorExpression> operators) {
        this.map = new LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression>();
        this.operators = new ObjList<OperatorExpression>();
        OperatorExpression dot = null, unaryMinus = null, unaryComplement = null, unarySetNegation = null;
        for (int i = 0, k = operators.size(); i < k; i++) {
            OperatorExpression op = operators.getQuick(i);
            switch (op.operator) {
                case UnaryMinus:
                    unaryMinus = op;
                    continue;
                case UnaryComplement:
                    unaryComplement = op;
                    continue;
                case UnarySetNegation:
                    unarySetNegation = op;
                    continue;
            }
            assert !map.contains(op.operator.token) : "unexpected operator conflict";
            map.put(op.operator.token, op);
            this.operators.add(op);
            if (op.operator == OperatorExpression.Operator.Dot) {
                dot = op;
            }
        }

        assert dot != null : "dot operator ('.') must be present in operators list";
        assert unaryMinus != null : "unary minus operator ('-') must be present in operators list";
        assert unaryComplement != null : "unary complement operator ('~') must be present in operators list";
        assert unarySetNegation != null : "unary set negation operator (e.g. 'not within') must be present in operators list";

        this.dot = dot;
        this.unaryMinus = unaryMinus;
        this.unaryComplement = unaryComplement;
        this.setOperationNegation = unarySetNegation;
    }

    public int getOperatorType(CharSequence name) {
        int index = map.keyIndex(name);
        if (index < 0) {
            return map.valueAt(index).type;
        }
        return 0;
    }

    public OperatorExpression tryGetOperator(OperatorExpression.Operator operator) {
        if (unaryMinus.operator == operator) {
            return unaryMinus;
        } else if (unaryComplement.operator == operator) {
            return unaryComplement;
        } else if (setOperationNegation.operator == operator) {
            return setOperationNegation;
        }
        return map.get(operator.token);
    }

    public OperatorExpression tryGuessOperator(CharSequence token, int precedence) {
        if (unaryMinus.operator.token.contentEquals(token) && unaryMinus.precedence == precedence) {
            return unaryMinus;
        }
        if (unaryComplement.operator.token.contentEquals(token) && unaryMinus.precedence == precedence) {
            return unaryComplement;
        }
        OperatorExpression op = map.get(token);
        if (op != null && op.precedence == precedence) {
            return op;
        }
        return null;
    }

    public boolean isOperator(CharSequence name) {
        return map.keyIndex(name) < 0;
    }
}
