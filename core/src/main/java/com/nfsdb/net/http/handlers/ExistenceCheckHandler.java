/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.net.http.handlers;

import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.misc.Chars;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.net.http.ResponseSink;

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
}
