/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;

public class RejectProcessorImpl implements RejectProcessor {
    protected final HttpConnectionContext httpConnectionContext;
    protected byte authenticationType = AUTH_TYPE_NONE;
    protected int rejectCode = 0;
    protected CharSequence rejectCookieName = null;
    protected CharSequence rejectCookieValue = null;
    protected CharSequence rejectMessage = null;

    public RejectProcessorImpl(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
    }

    @Override
    public void clear() {
        rejectCode = 0;
        authenticationType = AUTH_TYPE_NONE;
        rejectCookieName = null;
        rejectCookieValue = null;
        rejectMessage = null;
    }

    @Override
    public boolean isErrorProcessor() {
        return true;
    }

    @Override
    public boolean isRequestBeingRejected() {
        return rejectCode != 0;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        httpConnectionContext.simpleResponse().sendStatusWithCookie(rejectCode, rejectMessage, rejectCookieName, rejectCookieValue);
        httpConnectionContext.reset();
    }

    @Override
    public HttpRequestProcessor rejectRequest(int code, CharSequence userMessage, CharSequence cookieName, CharSequence cookieValue, byte authenticationType) {
        this.rejectCode = code;
        this.rejectMessage = userMessage;
        this.rejectCookieName = cookieName;
        this.rejectCookieValue = cookieValue;
        this.authenticationType = authenticationType;
        return this;
    }
}
