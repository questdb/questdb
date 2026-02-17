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

package io.questdb.griffin;

import io.questdb.std.LowerCaseAsciiCharSequenceObjHashMap;
import io.questdb.std.ObjList;

public class OperatorRegistry {
    public final LowerCaseAsciiCharSequenceObjHashMap<OperatorExpression> map;
    public final ObjList<OperatorExpression> operators;
    public OperatorExpression dot;
    public OperatorExpression unaryComplement;
    public OperatorExpression unaryMinus;
    public OperatorExpression unarySetNegation;

    public OperatorRegistry(ObjList<OperatorExpression> operators) {
        this.map = new LowerCaseAsciiCharSequenceObjHashMap<>();
        this.operators = new ObjList<>();
        for (int i = 0, k = operators.size(); i < k; i++) {
            OperatorExpression op = operators.getQuick(i);
            switch (op.operator) {
                // `continue` -- skip adding to registry; `break` -- proceed to add to registry.
                // Skip the unary operators because their token clashes with binary operators.
                // Binary operators (like dot) -- add them to the registry
                case UnaryMinus:
                    unaryMinus = op;
                    continue;
                case UnaryComplement:
                    unaryComplement = op;
                    continue;
                case UnarySetNegation:
                    unarySetNegation = op;
                    continue;
                case Dot:
                    dot = op;
                    break;
            }
            assert !map.contains(op.operator.token) : "unexpected operator conflict";
            map.put(op.operator.token, op);
            this.operators.add(op);
        }

        assert dot != null : "dot operator ('.') must be present in operators list";
        assert unaryMinus != null : "unary minus operator ('-') must be present in operators list";
        assert unaryComplement != null : "unary complement operator ('~') must be present in operators list";
        assert unarySetNegation != null : "unary set negation operator (e.g. 'not within') must be present in operators list";
    }

    public OperatorExpression getOperatorDefinition(CharSequence symbolName) {
        return map.get(symbolName);
    }

    public int getOperatorType(CharSequence name) {
        int index = map.keyIndex(name);
        if (index < 0) {
            return map.valueAt(index).type;
        }
        return 0;
    }

    public boolean isOperator(CharSequence name) {
        return map.contains(name);
    }

    /*
     * Returns the operator expression singleton (if it exists), after adjusting for duplicate unary operator tokens.
     * There are 3 unary operators that have duplicate token representation: '~', '-', 'not'.
     */
    public OperatorExpression tryGetOperator(OperatorExpression.Operator operator) {
        return operator == unaryMinus.operator ? unaryMinus
                : operator == unaryComplement.operator ? unaryComplement
                : operator == unarySetNegation.operator ? unarySetNegation
                : map.get(operator.token);
    }

    /*
     * Tries to choose operator from the registry only based on token & precedence. It takes into account
     * current registry structure with only 3 unary operators that have duplicate token representations: '~', '-', 'not'.
     * Although, as UnarySetNegation and BinaryNot operators have same precedence in the legacy registry, we can't
     * distinguish them from each other - so only UnaryMinus and UnaryComplement handled.
     * The method only required for the purpose of parsing behaviour validation between different registries:
     * see {@link ExpressionParser#parseExpr}.
     */
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
}
