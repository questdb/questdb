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

package com.questdb.ql.parser;

import com.questdb.ex.ParserException;
import com.questdb.std.str.CharSink;
import com.questdb.txt.sink.StringSink;

public final class QueryError implements QueryErrorBuilder {
    private static final QueryError INSTANCE = new QueryError();
    private final ThreadLocalDetails tl = new ThreadLocalDetails();

    private QueryError() {
    }

    public static ParserException $(int position, String message) {
        return position(position).$(message).$();
    }

    public static ParserException ambiguousColumn(int position) {
        return position(position).$("Ambiguous column name").$();
    }

    public static CharSequence getMessage() {
        return INSTANCE.tl.get().sink;
    }

    public static int getPosition() {
        return INSTANCE.tl.get().position;
    }

    public static ParserException invalidColumn(int position, CharSequence column) {
        return position(position).$("Invalid column: ").$(column).$();
    }

    public static QueryErrorBuilder position(int position) {
        Holder h = INSTANCE.tl.get();
        h.position = position;
        h.sink.clear();
        return INSTANCE;
    }

    @Override
    public ParserException $() {
        return ParserException.INSTANCE;
    }

    @Override
    public QueryErrorBuilder $(CharSequence sequence) {
        sink().put(sequence);
        return this;
    }

    @Override
    public QueryErrorBuilder $(int x) {
        sink().put(x);
        return this;
    }

    @Override
    public QueryErrorBuilder $(double x) {
        sink().put(x, 2);
        return this;
    }

    @Override
    public QueryErrorBuilder $(long x) {
        sink().put(x);
        return this;
    }

    @Override
    public QueryErrorBuilder $(char c) {
        sink().put(c);
        return this;
    }

    @Override
    public QueryErrorBuilder $(Enum e) {
        sink().put(e.name());
        return this;
    }

    private CharSink sink() {
        return tl.get().sink;
    }

    private static class Holder {
        private final StringSink sink = new StringSink();
        private int position;
    }

    private static class ThreadLocalDetails extends ThreadLocal<Holder> {
        @Override
        protected Holder initialValue() {
            return new Holder();
        }
    }
}
