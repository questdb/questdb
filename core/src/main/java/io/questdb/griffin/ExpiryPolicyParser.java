/*+*****************************************************************************
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

import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

/**
 * Turns the stored text of an EXPIRE ROWS policy into what binding reads: a filter expression, a LATEST ON, or the query
 * that computes the keep column of a window policy. Binding builds plan nodes from these and never handles the text.
 */
public final class ExpiryPolicyParser implements Mutable {
    private final SqlParser parser;
    // The tokens of a parse read its text until the next bind, whatever other reads of the query bind meanwhile, so
    // each parse has a text of its own.
    private final ObjectPool<StringSink> texts = new ObjectPool<>(StringSink::new, 2);

    ExpiryPolicyParser(SqlParser parser) {
        this.parser = parser;
    }

    @Override
    public void clear() {
        texts.clear();
    }

    /**
     * Parses the filter that keeps the rows a scalar policy has not expired, see {@link RowExpiryUtil#appendKeepFilter}.
     */
    public ExpressionNode parseKeepFilter(CharSequence predicate, SqlParserCallback parserCallback) throws SqlException {
        final StringSink text = texts.next();
        RowExpiryUtil.appendKeepFilter(text, predicate);
        return parser.parseExpiryExpression(text, parserCallback);
    }

    /**
     * Parses the LATEST ON of a KEEP LATEST policy into a model of its own.
     */
    public QueryModel parseKeepLatest(MetadataCache.ExpiryPolicyInfo policy, int position) throws SqlException {
        final StringSink text = texts.next();
        RowExpiryUtil.appendQuotedIdentifier(text, policy.getTimestampName());
        text.putAscii(" PARTITION BY ");
        RowExpiryUtil.appendKeepLatestKeys(text, policy.getPredicate());
        return parser.parseExpiryLatestBy(text, position);
    }

    /**
     * Parses the query over the whole table that computes the keep column of a window or keep-by policy. Its star is
     * composed here rather than written by whoever reads the view, so the model marks it as artificial.
     */
    public QueryModel parseKeepQuery(TableToken token, MetadataCache.ExpiryPolicyInfo policy, SqlParserCallback parserCallback) throws SqlException {
        final StringSink text = texts.next();
        text.putAscii("SELECT *, ");
        RowExpiryUtil.appendWindowKeepCase(text, policy.getPredicate(), policy.getTimestampName());
        text.putAscii(' ').putAscii(RowExpiryUtil.KEEP_COLUMN).putAscii(" FROM ");
        RowExpiryUtil.appendQuotedIdentifier(text, token.getTableName());
        final QueryModel query = parser.parseExpiryQuery(text, parserCallback);
        query.setArtificialStar(true);
        return query;
    }
}
