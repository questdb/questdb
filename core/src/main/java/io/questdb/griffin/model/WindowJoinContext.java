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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;

public class WindowJoinContext implements Mutable {
    public static final int CURRENT = 3;
    public static final int FOLLOWING = 2;
    public static final int PRECEDING = 1;
    private long hi = Long.MAX_VALUE;
    private ExpressionNode hiExpr;
    private int hiExprPos;
    private char hiExprTimeUnit;
    private int hiKind = CURRENT;
    private int hiKindPos;
    private boolean includePrevailing = true;
    private long lo = Long.MIN_VALUE;
    private ExpressionNode loExpr;
    private int loExprPos;
    private char loExprTimeUnit;
    private int loKind = PRECEDING;
    private int loKindPos;
    private QueryModel parentModel;
    private int prevailingPos;

    @Override
    public void clear() {
        loExpr = null;
        loExprPos = 0;
        lo = Long.MIN_VALUE;
        hi = Long.MAX_VALUE;
        loExprTimeUnit = 0;
        hiExpr = null;
        hiExprPos = 0;
        hiExprTimeUnit = 0;
        loKind = PRECEDING;
        loKindPos = 0;
        hiKind = CURRENT;
        hiKindPos = 0;
        includePrevailing = true;
        prevailingPos = 0;
        parentModel = null;
    }

    public long getHi() {
        return hi;
    }

    public ExpressionNode getHiExpr() {
        return hiExpr;
    }

    public int getHiExprPos() {
        return hiExprPos;
    }

    public char getHiExprTimeUnit() {
        return hiExprTimeUnit;
    }

    public int getHiKind() {
        return hiKind;
    }

    public int getHiKindPos() {
        return hiKindPos;
    }

    public long getLo() {
        return lo;
    }

    public ExpressionNode getLoExpr() {
        return loExpr;
    }

    public int getLoExprPos() {
        return loExprPos;
    }

    public char getLoExprTimeUnit() {
        return loExprTimeUnit;
    }

    public int getLoKind() {
        return loKind;
    }

    public int getLoKindPos() {
        return loKindPos;
    }

    public QueryModel getParentModel() {
        return parentModel;
    }

    public int getPrevailingPos() {
        return prevailingPos;
    }

    public boolean isIncludePrevailing() {
        return includePrevailing;
    }

    public void setHi(long hi) {
        this.hi = hi;
    }

    public void setHiExpr(ExpressionNode hiExpr, int hiExprPos) {
        this.hiExpr = hiExpr;
        this.hiExprPos = hiExprPos;
    }

    public void setHiExprTimeUnit(char hiExprTimeUnit) {
        this.hiExprTimeUnit = hiExprTimeUnit;
    }

    public void setHiKind(int hiKind, int hiKindPos) {
        this.hiKind = hiKind;
        this.hiKindPos = hiKindPos;
    }

    public void setIncludePrevailing(boolean includePrevailing, int pos) {
        this.includePrevailing = includePrevailing;
        this.prevailingPos = pos;
    }

    public void setLo(long lo) {
        this.lo = lo;
    }

    public void setLoExpr(ExpressionNode loExpr, int loExprPos) {
        this.loExpr = loExpr;
        this.loExprPos = loExprPos;
    }

    public void setLoExprTimeUnit(char loExprTimeUnit) {
        this.loExprTimeUnit = loExprTimeUnit;
    }

    public void setLoKind(int loKind, int loKindPos) {
        this.loKind = loKind;
        this.loKindPos = loKindPos;
    }

    public void setParentModel(QueryModel parentModel) {
        this.parentModel = parentModel;
    }
}
