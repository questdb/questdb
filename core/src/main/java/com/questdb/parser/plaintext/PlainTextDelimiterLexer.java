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

package com.questdb.parser.plaintext;

import com.questdb.std.IntList;
import com.questdb.std.IntLongPriorityQueue;
import com.questdb.std.ObjectFactory;
import com.questdb.std.Unsafe;

import static com.questdb.parser.plaintext.PlainTextDelimiter.*;

public class PlainTextDelimiterLexer {
    public static final ObjectFactory<PlainTextDelimiterLexer> FACTORY = PlainTextDelimiterLexer::new;
    private static final int maxLines = 10000;
    private final IntList commas = new IntList(maxLines);
    private final IntList pipes = new IntList(maxLines);
    private final IntList tabs = new IntList(maxLines);
    private final IntList semicolons = new IntList(maxLines);
    private final IntLongPriorityQueue heap = new IntLongPriorityQueue();
    private double stdDev;
    private int avgRecLen;
    private char delimiter;

    private PlainTextDelimiterLexer() {
    }

    public char getDelimiter() {
        return delimiter;
    }

    public double getStdDev() {
        return stdDev;
    }

    @SuppressWarnings("ConstantConditions")
    public void of(long address, int len) {
        long lim = address + len;
        long p = address;
        boolean suspended = false;
        int line = 0;

        int comma = 0;
        int pipe = 0;
        int tab = 0;
        int semicolon = 0;

        // previous values
        int _comma = 0;
        int _pipe = 0;
        int _tab = 0;
        int _semicolon = 0;

        commas.clear();
        pipes.clear();
        tabs.clear();
        semicolons.clear();

        this.avgRecLen = 0;
        this.stdDev = Double.POSITIVE_INFINITY;
        this.delimiter = 0;

        boolean eol = false;
        while (p < lim && line < maxLines) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (suspended && b != '"') {
                continue;
            }

            switch (b) {
                case ',':
                    comma++;
                    if (eol) {
                        eol = false;
                    }
                    break;
                case '|':
                    pipe++;
                    if (eol) {
                        eol = false;
                    }
                    break;
                case '\t':
                    tab++;
                    if (eol) {
                        eol = false;
                    }
                    break;
                case ';':
                    semicolon++;
                    if (eol) {
                        eol = false;
                    }
                    break;
                case '"':
                    suspended = !suspended;
                    if (eol) {
                        eol = false;
                    }
                    break;
                case '\n':
                    if (eol) {
                        break;
                    }

                    line++;
                    commas.add(comma - _comma);
                    pipes.add(pipe - _pipe);
                    tabs.add(tab - _tab);
                    semicolons.add(semicolon - _semicolon);

                    _comma = comma;
                    _pipe = pipe;
                    _tab = tab;
                    _semicolon = semicolon;

                    eol = true;
                    break;
                default:
                    if (eol) {
                        eol = false;
                    }
                    break;
            }
        }

        if (line == 0) {
            return;
        }

        this.avgRecLen = len / line;

        heap.clear();
        heap.add(CSV, comma);
        heap.add(PIPE, pipe);
        heap.add(TAB, tab);
        heap.add(';', semicolon);

        this.delimiter = (char) heap.peekBottom();
        IntList test;

        switch (delimiter) {
            case CSV:
                test = commas;
                break;
            case PIPE:
                test = pipes;
                break;
            case TAB:
                test = tabs;
                break;
            case ';':
                test = semicolons;
                break;
            default:
                throw new IllegalArgumentException("Unsupported delimiter: " + delimiter);
        }

        // compute variance on test delimiter

        double temp;
        int n = test.size();

        if (n == 0) {
            delimiter = 0;
            return;
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
    }

    int getAvgRecLen() {
        return avgRecLen;
    }
}
