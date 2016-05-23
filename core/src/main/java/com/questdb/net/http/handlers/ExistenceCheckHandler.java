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

package com.questdb.net.http.handlers;

import com.questdb.factory.JournalFactory;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.misc.Chars;
import com.questdb.net.http.ContextHandler;
import com.questdb.net.http.IOContext;
import com.questdb.net.http.ResponseSink;

import java.io.IOException;

public class ExistenceCheckHandler implements ContextHandler {

    private final JournalConfiguration configuration;

    public ExistenceCheckHandler(final JournalFactory factory) {
        this.configuration = factory.getConfiguration();
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence journalName = context.request.getUrlParam("j");
        if (journalName == null) {
            context.simpleResponse().send(400);
        } else {
            JournalConfiguration.JournalExistenceCheck check = configuration.exists(journalName);
            if (Chars.equalsNc("json", context.request.getUrlParam("f"))) {
                ResponseSink r = context.responseSink();
                r.status(200, "application/json");
                r.put('{').putQuoted("status").put(':').putQuoted(check.name()).put('}');
                r.flush();
            } else {
                context.simpleResponse().send(200, check.name());
            }
        }
    }

    @Override
    public void resume(IOContext context) throws IOException {
        // nothing to do
    }

    @Override
    public void setupThread() {
    }
}
