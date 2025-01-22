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
import io.questdb.cutlass.http.HttpResponseSink;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

public class RejectProcessorImpl implements RejectProcessor {
    private static final Log LOG = LogFactory.getLog(RejectProcessorImpl.class);
    protected final HttpConnectionContext httpConnectionContext;
    private final StringSink rejectMessage = new StringSink();
    protected byte authenticationType = AUTH_TYPE_NONE;
    protected int rejectCode = 0;
    protected CharSequence rejectCookieName = null;
    protected CharSequence rejectCookieValue = null;
    protected boolean shutdownWrite = false;

    public RejectProcessorImpl(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
    }

    @Override
    public void clear() {
        rejectCode = 0;
        authenticationType = AUTH_TYPE_NONE;
        rejectCookieName = null;
        rejectCookieValue = null;
        rejectMessage.clear();
        shutdownWrite = false;
    }

    @Override
    public Utf16Sink getMessageSink() {
        return rejectMessage;
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
        final HttpResponseSink.SimpleResponseImpl response = httpConnectionContext.simpleResponse();
        if (rejectCode == HTTP_UNAUTHORIZED) {
            handleHttpUnauthorized(response);
        } else {
            response.sendStatusWithCookie(rejectCode, rejectMessage, rejectCookieName, rejectCookieValue);
        }

        if (shutdownWrite) {
            response.shutdownWrite();
        }
        httpConnectionContext.reset();
    }

    @Override
    public RejectProcessor reject(int rejectCode) {
        LOG.error().$("rejecting request [code=").$(rejectCode).I$();
        this.rejectCode = rejectCode;
        return this;
    }

    @Override
    public RejectProcessor reject(int rejectCode, CharSequence rejectMessage) {
        LOG.error().$(rejectMessage).$(" [code=").$(rejectCode).I$();
        this.rejectCode = rejectCode;
        this.rejectMessage.put(rejectMessage);
        return this;
    }

    @Override
    public RejectProcessor withAuthenticationType(byte authenticationType) {
        this.authenticationType = authenticationType;
        return this;
    }

    @Override
    public RejectProcessor withCookie(CharSequence cookieName, CharSequence cookieValue) {
        this.rejectCookieName = cookieName;
        this.rejectCookieValue = cookieValue;
        return this;
    }

    @Override
    public RejectProcessor withShutdownWrite() {
        this.shutdownWrite = true;
        return this;
    }

    protected void handleHttpUnauthorized(
            HttpResponseSink.SimpleResponseImpl response
    ) throws PeerIsSlowToReadException, PeerDisconnectedException {
        response.sendStatusTextContent(HTTP_UNAUTHORIZED);
    }
}
