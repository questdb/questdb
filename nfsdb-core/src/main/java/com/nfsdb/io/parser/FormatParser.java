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

package com.nfsdb.io.parser;

import com.nfsdb.collections.IntList;
import com.nfsdb.collections.IntLongPriorityQueue;
import com.nfsdb.io.TextFileFormat;
import com.nfsdb.misc.Unsafe;

public class FormatParser {
    private static final int maxLines = 10000;
    private final IntList commas = new IntList(maxLines);
    private final IntList pipes = new IntList(maxLines);
    private final IntList tabs = new IntList(maxLines);
    private final IntLongPriorityQueue heap = new IntLongPriorityQueue(3);

    private double stdDev;
    private int avgRecLen;
    private TextFileFormat format;

    public int getAvgRecLen() {
        return avgRecLen;
    }

    public TextFileFormat getFormat() {
        return format;
    }

    public double getStdDev() {
        return stdDev;
    }

    @SuppressWarnings("ConstantConditions")
    public FormatParser of(long address, int len) {
        long lim = address + len;
        long p = address;
        boolean suspended = false;
        int line = 0;

        int comma = 0;
        int pipe = 0;
        int tab = 0;

        // previous values
        int _comma = 0;
        int _pipe = 0;
        int _tab = 0;

        commas.clear();
        pipes.clear();
        tabs.clear();

        this.avgRecLen = 0;
        this.stdDev = Double.POSITIVE_INFINITY;
        this.format = null;

        while (p < lim) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (suspended && b != '"') {
                continue;
            }

            switch (b) {
                case ',':
                    comma++;
                    break;
                case '|':
                    pipe++;
                    break;
                case '\t':
                    tab++;
                    break;
                case '"':
                    suspended = !suspended;
                    break;
                case '\n':
                    line++;
                    commas.add(comma - _comma);
                    pipes.add(pipe - _pipe);
                    tabs.add(tab - _tab);

                    _comma = comma;
                    _pipe = pipe;
                    _tab = tab;
                    break;
                default:
                    break;
            }
        }

        if (comma > _comma || pipe > _pipe || tab > _tab) {
            line++;
            commas.add(comma - _comma);
            pipes.add(pipe - _pipe);
            tabs.add(tab - _tab);
        }

        if (line == 0) {
            return this;
        }

        this.avgRecLen = len / line;

        heap.clear();
        heap.add(TextFileFormat.CSV.ordinal(), comma);
        heap.add(TextFileFormat.PIPE.ordinal(), pipe);
        heap.add(TextFileFormat.TAB.ordinal(), tab);

        this.format = TextFileFormat.values()[heap.peekBottom()];
        IntList test;

        switch (format) {
            case CSV:
                test = commas;
                break;
            case PIPE:
                test = pipes;
                break;
            case TAB:
                test = tabs;
                break;
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }

        // compute variance on test delimiter

        double temp;
        int n = test.size();

        if (n == 0) {
            format = null;
            return this;
        }

        temp = 0;
        for (int i = 0; i < n; i++) {
            temp += test.getQuick(i);
        }

        double mean = temp / n;

        temp = 0;
        for (int i = 0; i < n; i++) {
            int v = test.getQuick(i);
            temp += (mean - v) * (mean - v);
        }

        this.stdDev = Math.sqrt(temp / n);
        return this;
    }
}
