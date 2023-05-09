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

package io.questdb.cutlass.pgwire;

import io.questdb.std.Mutable;
import io.questdb.std.str.DirectByteCharSequence;

public class ClearTextPgWireAuthenticator implements Mutable, PgWireAuthenticator {
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final PGConnectionContext.ResponseAsciiSink responseAsciiSink;
    private final PgWireUserDatabase userDatabase;
    private CharSequence principal;
    private State state = State.SEND_LOGIN_REQUEST;

    public ClearTextPgWireAuthenticator(PGConnectionContext.ResponseAsciiSink responseAsciiSink, PGWireConfiguration configuration) {
        this(responseAsciiSink, new StaticUserDatabase(configuration));
    }

    public ClearTextPgWireAuthenticator(PGConnectionContext.ResponseAsciiSink responseAsciiSink, PgWireUserDatabase userDatabase) {
        this.responseAsciiSink = responseAsciiSink;
        this.userDatabase = userDatabase;
    }

    @Override
    public void clear() {
        state = State.SEND_LOGIN_REQUEST;
    }


    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    @Override
    public boolean isAuthenticated() {
        return state == State.SUCCESS;
    }

    @Override
    public AuthenticationResult onAfterInitMessage() {
        assert state == State.SEND_LOGIN_REQUEST;
        state = State.READ_LOGIN_RESPONSE;
        prepareLoginResponse();
        return AuthenticationResult.NEED_READ;
    }

    @Override
    public AuthenticationResult processMessage(CharSequence usernameFromInitMessage, long msgStart, long msgLimit) throws BadProtocolException {
        assert state == State.READ_LOGIN_RESPONSE;
        assert msgLimit > msgStart;
        long hi = PGConnectionContext.getStringLength(msgStart, msgLimit, "bad password length");
        dbcs.of(msgStart, hi);
        if (userDatabase.match(usernameFromInitMessage, dbcs)) {
            principal = usernameFromInitMessage;
            state = State.SUCCESS;
            return AuthenticationResult.AUTHENTICATION_SUCCESS;
        } else {
            state = State.FAILED;
            return AuthenticationResult.AUTHENTICATION_FAILED;
        }
    }

    private void prepareLoginResponse() {
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(Integer.BYTES * 2);
        responseAsciiSink.putNetworkInt(3); // clear text password
    }

    public enum State { // says what we should do next
        SEND_LOGIN_REQUEST,
        READ_LOGIN_RESPONSE,
        SUCCESS,
        FAILED
    }
}
