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

package com.questdb.tuck.http.handlers;

import com.questdb.BootstrapEnv;
import com.questdb.std.Chars;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.tuck.http.ContextHandler;
import com.questdb.tuck.http.IOContext;
import com.questdb.tuck.http.ResponseSink;

import java.io.IOException;

public class ExistenceCheckHandler implements ContextHandler {

    private final JournalConfiguration configuration;

    public ExistenceCheckHandler(BootstrapEnv env) {
        this.configuration = env.factory.getConfiguration();
    }

    @Override
    public void handle(IOContext context) throws IOException {
        CharSequence journalName = context.request.getUrlParam("j");
        if (journalName == null) {
            context.simpleResponse().send(400);
        } else {
            int check = configuration.exists(journalName);
            if (Chars.equalsNc("json", context.request.getUrlParam("f"))) {
                ResponseSink r = context.responseSink();
                r.status(200, "application/json");
                r.put('{').putQuoted("status").put(':').putQuoted(toResponse(check)).put('}');
                r.flush();
            } else {
                context.simpleResponse().send(200, toResponse(check));
            }
        }
    }

    @Override
    public void resume(IOContext context) {
        // nothing to do
    }

    @Override
    public void setupThread() {
    }

    private static String toResponse(int existenceCheckResult) {
        switch (existenceCheckResult) {
            case JournalConfiguration.EXISTS:
                return "Exists";
            case JournalConfiguration.DOES_NOT_EXIST:
                return "Does not exist";
            case JournalConfiguration.EXISTS_FOREIGN:
                return "Reserved name";
            default:
                return "Unknown";
        }
    }
}
