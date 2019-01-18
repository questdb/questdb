/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.ops.regex;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.ConcatCharSequence;
import com.questdb.std.str.FlyweightCharSequence;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

class ReplaceStrFunction extends AbstractVirtualColumn implements Function {
    private FlyweightCharSequence left;
    private FlyweightCharSequence right;
    private ConcatCharSequence replacePattern;
    private VirtualColumn value;
    private Matcher matcher;
    private CharSequence base;
    private boolean trivial;

    public ReplaceStrFunction(int position) {
        super(ColumnType.STRING, position);
    }

    public static void main(String[] args) {
        String a = "2015-12-20";

        System.out.println(a.replaceAll("(.+)-", "x"));
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        base = value.getFlyweightStr(rec);
        boolean found = matcher.reset(base).find();
        if (trivial) {
            if (found) {
                left.of(base, 0, matcher.start());
                right.of(base, matcher.end(), base.length() - matcher.end());
            } else {
                return base;
            }
        }
        replacePattern.computeLen();
        return replacePattern;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(getFlyweightStr(rec));
    }

    @Override
    public boolean isConstant() {
        return value.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                compileRegex(arg);
                break;
            case 1:
                compileReplacePattern(arg);
                break;
            case 2:
                value = arg;
                break;
            default:
                throw QueryError.$(arg.getPosition(), "unexpected argument");
        }
    }

    private void compileRegex(VirtualColumn arg) throws ParserException {
        CharSequence pattern = arg.getFlyweightStr(null);
        if (pattern == null) {
            throw QueryError.$(arg.getPosition(), "null regex?");
        }
        try {
            matcher = Pattern.compile(pattern.toString()).matcher("");
        } catch (PatternSyntaxException e) {
            throw QueryError.position(arg.getPosition() + e.getIndex() + 2 /* zero based index + quote symbol*/).$("Regex syntax error. ").$(e.getDescription()).$();
        }
    }

    private void compileReplacePattern(VirtualColumn arg) throws ParserException {
        CharSequence pattern = arg.getFlyweightStr(null);
        if (pattern == null) {
            throw QueryError.$(arg.getPosition(), "null pattern?");
        }

        int pos = arg.getPosition();
        int start = 0;
        int index = -1;
        int dollar = -2;
        int dollarCount = 0;

        ConcatCharSequence concat = new ConcatCharSequence();
        boolean collectIndex = false;
        int n = pattern.length();

        for (int i = 0; i < n; i++) {
            char c = pattern.charAt(i);
            if (c == '$') {
                if (i == dollar + 1) {
                    throw QueryError.$(pos + i, "missing index");
                }
                if (i > start) {
                    concat.add(new FlyweightCharSequence().of(pattern, start, i - start));
                }
                collectIndex = true;
                index = 0;
                dollar = i;
                dollarCount++;
            } else {
                if (collectIndex) {
                    int k = c - '0';
                    if (k > -1 && k < 10) {
                        index = index * 10 + k;
                    } else {
                        if (i == dollar + 1) {
                            throw QueryError.$(pos + i, "missing index");
                        }
                        concat.add(new GroupCharSequence(index));
                        start = i;
                        collectIndex = false;
                        index = -1;
                    }
                }
            }
        }

        if (collectIndex) {
            if (n == dollar + 1) {
                throw QueryError.$(pos + n, "missing index");
            }
            concat.add(new GroupCharSequence(index));
        } else if (start < n) {
            concat.add(new FlyweightCharSequence().of(pattern, start, n - start));
        }

        if (trivial = (dollarCount == 0)) {
            left = new FlyweightCharSequence();
            right = new FlyweightCharSequence();
            concat.surroundWith(left, right);
        }
        this.replacePattern = concat;
    }

    private class GroupCharSequence extends AbstractCharSequence {
        private final int lo;
        private final int hi;
        private final int max;

        public GroupCharSequence(int group) {
            this.lo = group * 2;
            this.hi = this.lo + 1;
            this.max = group - 1;
        }

        @Override
        public int length() {
            if (base == null) {
                return 0;
            }

            if (max < matcher.groupCount()) {
                int l = matcher.groupQuick(lo);
                int h = matcher.groupQuick(hi);
                return h - l;
            } else {
                return 0;
            }
        }

        @Override
        public char charAt(int index) {
            return base.charAt(index + matcher.groupQuick(lo));
        }
    }
}
