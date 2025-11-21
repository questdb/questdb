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

package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.str.StringSink;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Serves OpenAPI 3.0 specification for the /api/v1 endpoints.
 * The specification is loaded from the classpath resource openapi-v1.yaml.
 */
public class OpenApiProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(OpenApiProcessor.class);
    private static final String OPENAPI_RESOURCE = "/openapi-v1.yaml";
    private static final String CONTENT_TYPE = "application/yaml";
    private String openApiSpec;

    public OpenApiProcessor() {
        loadOpenApiSpec();
    }

    private void loadOpenApiSpec() {
        try (InputStream stream = getClass().getResourceAsStream(OPENAPI_RESOURCE)) {
            if (stream == null) {
                LOG.info().$("OpenAPI specification resource not found: ").$(OPENAPI_RESOURCE).$();
                openApiSpec = "# OpenAPI specification not found";
                return;
            }

            StringSink sink = new StringSink();
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = stream.read(buffer)) != -1) {
                sink.put(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
            }
            openApiSpec = sink.toString();
            LOG.info().$("Loaded OpenAPI specification (").$(openApiSpec.length()).$(") bytes").$();
        } catch (IOException e) {
            LOG.error().$("Failed to load OpenAPI specification: ").$(e).$();
            openApiSpec = "# Error loading OpenAPI specification";
        }
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public short getSupportedRequestTypes() {
        return METHOD_GET;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException,
            PeerIsSlowToReadException, ServerDisconnectException {
        final HttpChunkedResponse response = context.getChunkedResponse();
        response.status(HTTP_OK, CONTENT_TYPE);
        response.sendHeader();
        response.putAscii(openApiSpec);
        response.done();
    }
}
