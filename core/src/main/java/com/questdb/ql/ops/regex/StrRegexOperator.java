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
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;


public class StrRegexOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new StrRegexOperator(position);

    private Matcher matcher;

    private StrRegexOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        CharSequence cs = lhs.getFlyweightStr(rec);
        return cs != null && matcher.reset(cs).find();
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        super.setRhs(rhs);
        CharSequence pattern = rhs.getFlyweightStr(null);
        if (pattern == null) {
            throw QueryError.$(rhs.getPosition(), "null regex?");
        }

        try {
            matcher = Pattern.compile(pattern.toString()).matcher("");
        } catch (PatternSyntaxException e) {
            throw QueryError.position(rhs.getPosition() + e.getIndex() + 2 /* zero based index + quote symbol*/).$("Regex syntax error. ").$(e.getDescription()).$();
        }
    }
}
