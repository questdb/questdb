/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql.parser;

import com.questdb.ex.ParserException;
import com.questdb.io.sink.CharSink;
import com.questdb.io.sink.StringSink;

public final class QueryError implements QueryErrorBuilder {
    private static final QueryError INSTANCE = new QueryError();
    private final ThreadLocalDetails tl = new ThreadLocalDetails();

    private QueryError() {
    }

    public static ParserException $(int position, String message) {
        return position(position).$(message).$();
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
