/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb;

import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.DefaultAuthenticatorFactory;
import io.questdb.cutlass.auth.AuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGBasicAuthenticatorFactory;
import io.questdb.griffin.SqlParserFactory;
import io.questdb.griffin.SqlParserFactoryImpl;

public class DefaultFactoryProvider implements FactoryProvider {
    public static final DefaultFactoryProvider INSTANCE = new DefaultFactoryProvider();

    @Override
    public PGAuthenticatorFactory getPGAuthenticatorFactory() {
        return PGBasicAuthenticatorFactory.INSTANCE;
    }

    @Override
    public SecurityContextFactory getSecurityContextFactory() {
        return AllowAllSecurityContextFactory.INSTANCE;
    }

    @Override
    public SqlParserFactory getSqlParserFactory() {
        return SqlParserFactoryImpl.INSTANCE;
    }

    @Override
    public AuthenticatorFactory getAuthenticatorFactory() {
        return DefaultAuthenticatorFactory.INSTANCE;
    }
}
