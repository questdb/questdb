/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.std.ConcatCharSequence;
import com.questdb.std.FlyweightCharSequence;

public class DestPatternParser {
    public static void main(String[] args) {
        System.out.println(parse("this is $8 awesome"));
    }

    public static CharSequence parse(CharSequence pattern) {
        int start = 0;
        int index = -1;
        int dollar = -2;

        ConcatCharSequence concat = new ConcatCharSequence();
        boolean collectIndex = false;
        int n = pattern.length();

        for (int i = 0; i < n; i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '$':
                    if (i == dollar + 1) {
                        throw new IllegalArgumentException("missing index");
                    }
                    if (i > start) {
                        concat.add(new FlyweightCharSequence().of(pattern, start, i - start));
                    }
                    collectIndex = true;
                    index = 0;
                    dollar = i;
                    break;
                default:
                    if (collectIndex) {
                        int k = c - '0';
                        if (k > -1 && k < 10) {
                            index = index * 10 + k;
                        } else {
                            if (i == dollar + 1) {
                                throw new IllegalArgumentException("missing index");
                            }
                            concat.add("~" + index + "~");
                            start = i;
                            collectIndex = false;
                            index = -1;
                        }
                    }
                    break;
            }
        }
        if (start < n) {
            concat.add(new FlyweightCharSequence().of(pattern, start, n - start));
        }

        return concat;
    }
}
