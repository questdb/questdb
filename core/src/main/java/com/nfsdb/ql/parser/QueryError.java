/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.ex.ParserException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.StringSink;

public final class QueryError implements QueryErrorBuilder {
    public static final QueryError INSTANCE = new QueryError();
    private final ThreadLocalDetails tl = new ThreadLocalDetails();

    private QueryError() {
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

    public ParserException $(int position, String message) {
        return position(position).$(message).$();
    }

    public ParserException invalidColumn(int position) {
        return $(position, "Invalid column");
    }

    public CharSequence getMessage() {
        return tl.get().sink;
    }

    public int getPosition() {
        return tl.get().position;
    }

    public QueryErrorBuilder position(int position) {
        Holder h = tl.get();
        h.position = position;
        h.sink.clear();
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
