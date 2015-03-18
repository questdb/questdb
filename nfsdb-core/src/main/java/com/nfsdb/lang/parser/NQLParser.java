/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.lang.parser;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.GenericIntBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.lang.ast.Statement;
import com.nfsdb.lang.ast.StatementType;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;

public class NQLParser {

    private final TokenStream tokenStream = new TokenStream() {{
        defineSymbol(new Token(" "));
        defineSymbol(new Token("("));
        defineSymbol(new Token(")"));
        defineSymbol(new Token(","));
    }};

    public static void main(String[] args) throws ParserException, JournalException {
        NQLParser parser = new NQLParser();
        Statement statement = parser.parse("create journal xyz (a INT index buckets 250, b DOUBLE)");

        JournalFactory factory = new JournalFactory("d:/data");
        JournalWriter w = factory.writer(statement.getStructure());
        w.close();
    }

    public Statement parse(CharSequence cs) throws ParserException {
        tokenStream.setContent(cs);

        CharSequence tok = tok();
        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }
        throw new ParserException("Unexpected token:" + tok);
    }

    private CharSequence tok() throws ParserException {
        while (tokenStream.hasNext()) {
            CharSequence cs = tokenStream.next();
            if (!Chars.equals(cs, ' ')) {
                return cs;
            }
        }

        throw new ParserException("Unexpected end of input");
    }

    private Statement parseCreateStatement() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "journal")) {
            return parseCreateJournal();
        }

        throw new ParserException("Unexpected token:" + tok);
    }

    private Statement parseCreateJournal() throws ParserException {
        Statement statement = new Statement();
        statement.setType(StatementType.CREATE_JOURNAL);
        JournalStructure structure = new JournalStructure(tok().toString());
        statement.setStructure(structure);

        parseJournalFields(structure);

        return statement;
    }

    private void parseJournalFields(JournalStructure struct) throws ParserException {
        if (!Chars.equals(tok(), '(')) {
            throw new ParserException("( expected");
        }

        while (true) {
            String name = notTermTok();
            CharSequence tok = null;
            switch (ColumnType.valueOf(notTermTok())) {
                case INT:
                    tok = parseIntDefinition(struct.$int(name));
                    break;
                case DOUBLE:
                    struct.$double(name);
                    break;
                case BOOLEAN:
                    struct.$bool(name);
                    break;
                case FLOAT:
                    struct.$float(name);
                    break;
                case LONG:
                    struct.$long(name);
                    break;
                case SHORT:
                    struct.$short(name);
                    break;
                case STRING:
                    struct.$str(name);
                    break;
                case SYMBOL:
                    struct.$sym(name);
                    break;
                case BINARY:
                    struct.$bin(name);
                    break;
                case DATE:
                    struct.$date(name);
            }

            if (tok == null) {
                tok = tok();
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw new ParserException(", expected");
            }
        }
    }

    private CharSequence parseIntDefinition(GenericIntBuilder genericIntBuilder) throws ParserException {
        CharSequence tok = tok();

        if (isFieldTerm(tok)) {
            return tok;
        }

        if (!Chars.equals(tok, "index")) {
            throw new ParserException("\"index\" expected, found: " + tok);
        }

        genericIntBuilder.index();

        if (isFieldTerm(tok = tok())) {
            return tok;
        }

        if (!Chars.equals(tok, "buckets")) {
            throw new ParserException("\"buckets\" expected, found: " + tok);
        }

        try {
            genericIntBuilder.buckets(Numbers.parseInt(tok = tok()));
        } catch (NumberFormatException e) {
            throw new ParserException("expected number of buckets (int) but found: " + tok);
        }

        return null;
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private String notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw new ParserException("Invalid column definition");
        }
        return tok.toString();
    }
}
