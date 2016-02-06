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

package com.nfsdb.net.http;

import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.SslConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

@SuppressFBWarnings("PATH_TRAVERSAL_IN")
public class HttpServerConfiguration {
    private final SslConfig sslConfig = new SslConfig();
    private int httpPort = 9000;
    private int httpBufReqHeader = 64 * 1024;
    private int httpBufReqContent = 4 * 1024 * 1024;
    private int httpBufReqMultipart = 1024;
    private int httpBufRespHeader = 1024;
    private int httpBufRespContent = 1024 * 1024;
    private int httpThreads = 2;
    private int httpTimeout = 10000;
    private int httpMaxConnections = 128;
    private File dbPath = new File("db");
    private File mimeTypes = new File("conf/mime.types");
    private File httpPublic = new File("public");
    private File accessLog = new File("log/access.log");
    private File errorLog = new File("log/error.log");

    public HttpServerConfiguration() {
    }

    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
    public HttpServerConfiguration(File nfsdbConf) throws Exception {

        final Properties props = new Properties();
        final File root = nfsdbConf.getParentFile().getParentFile();

        try (FileInputStream fis = new FileInputStream(nfsdbConf)) {
            props.load(fis);
        }

        int n;

        if ((n = parseInt(props, "http.port")) > -1) {
            this.httpPort = n;
        }

        if ((n = parseInt(props, "http.threads")) > -1) {
            this.httpThreads = n;
        }

        if ((n = parseInt(props, "http.timeout")) > -1) {
            this.httpTimeout = n;
        }

        if ((n = parseInt(props, "http.max.connections")) > -1) {
            this.httpMaxConnections = n;
        }

        if ((n = parseSize(props, "http.buf.req.header")) > -1) {
            this.httpBufReqHeader = n;
        }

        if ((n = parseSize(props, "http.buf.req.content")) > -1) {
            this.httpBufReqContent = n;
        }

        if ((n = parseSize(props, "http.buf.req.multipart")) > -1) {
            this.httpBufReqMultipart = n;
        }

        if ((n = parseSize(props, "http.buf.resp.header")) > -1) {
            this.httpBufRespHeader = n;
        }

        if ((n = parseSize(props, "http.buf.resp.content")) > -1) {
            this.httpBufRespContent = n;
        }

        String s;

        if ((s = props.getProperty("db.path")) != null) {
            this.dbPath = mkdirs(normalize(root, new File(s)));
        } else {
            this.dbPath = mkdirs(normalize(root, this.dbPath));
        }

        if ((s = props.getProperty("mime.types")) != null) {
            this.mimeTypes = normalize(root, new File(s));
        } else {
            this.mimeTypes = normalize(root, mimeTypes);
        }

        if ((s = props.getProperty("http.public")) != null) {
            this.httpPublic = mkdirs(normalize(root, new File(s)));
        } else {
            this.httpPublic = mkdirs(normalize(root, this.httpPublic));
        }

        if ((s = props.getProperty("http.log.access")) != null) {
            this.accessLog = normalize(root, new File(s));
        } else {
            this.accessLog = normalize(root, this.accessLog);
        }
        mkdirs(this.accessLog.getParentFile());

        if ((s = props.getProperty("http.log.error")) != null) {
            this.errorLog = normalize(root, new File(s));
        } else {
            this.errorLog = normalize(root, this.errorLog);
        }
        mkdirs(this.accessLog.getParentFile());


        // SSL section
        sslConfig.setSecure("true".equals(props.getProperty("http.ssl.enabled")));
        if (sslConfig.isSecure()) {
            if ((s = props.getProperty("http.ssl.keystore.location")) == null) {
                throw new IllegalArgumentException("http.ssl.keystore.location is undefined");
            }

            File keystore = normalize(root, new File(s));

            if (!keystore.exists()) {
                throw new IllegalArgumentException("http.ssl.keystore.location does not exist");
            }

            if (!keystore.isFile()) {
                throw new IllegalArgumentException("http.ssl.keystore.location is not a file");
            }

            try (InputStream is = new FileInputStream(keystore)) {
                s = props.getProperty("http.ssl.keystore.password");
                sslConfig.setKeyStore(is, s == null ? "" : s);
            }
            sslConfig.setRequireClientAuth("true".equals(props.getProperty("http.ssl.auth")));
        }
    }

    public File getAccessLog() {
        return accessLog;
    }

    public File getDbPath() {
        return dbPath;
    }

    public File getErrorLog() {
        return errorLog;
    }

    public int getHttpBufReqContent() {
        return httpBufReqContent;
    }

    public int getHttpBufReqHeader() {
        return httpBufReqHeader;
    }

    public int getHttpBufReqMultipart() {
        return httpBufReqMultipart;
    }

    public int getHttpBufRespContent() {
        return httpBufRespContent;
    }

    public void setHttpBufRespContent(int httpBufRespContent) {
        this.httpBufRespContent = httpBufRespContent;
    }

    public int getHttpBufRespHeader() {
        return httpBufRespHeader;
    }

    public int getHttpMaxConnections() {
        return httpMaxConnections;
    }

    public void setHttpMaxConnections(int httpMaxConnections) {
        this.httpMaxConnections = httpMaxConnections;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public File getHttpPublic() {
        return httpPublic;
    }

    public int getHttpThreads() {
        return httpThreads;
    }

    public int getHttpTimeout() {
        return httpTimeout;
    }

    public void setHttpTimeout(int httpTimeout) {
        this.httpTimeout = httpTimeout;
    }

    public File getMimeTypes() {
        return mimeTypes;
    }

    public SslConfig getSslConfig() {
        return sslConfig;
    }

    @Override
    public String toString() {
        return "HttpServerConfiguration{" +
                "\n\thttpPort=" + httpPort +
                ",\n\thttpBufReqHeader=" + httpBufReqHeader +
                ",\n\thttpBufReqContent=" + httpBufReqContent +
                ",\n\thttpBufReqMultipart=" + httpBufReqMultipart +
                ",\n\thttpBufRespHeader=" + httpBufRespHeader +
                ",\n\thttpBufRespContent=" + httpBufRespContent +
                ",\n\thttpThreads=" + httpThreads +
                ",\n\tdbPath=" + dbPath +
                "\n}";
    }

    private File mkdirs(File dir) throws IOException {
        if (!dir.exists()) {
            Files.createDirectories(dir.toPath());
        }
        return dir;
    }

    private File normalize(File root, File file) {
        if (file.isAbsolute()) {
            return file;
        }
        return new File(root, file.getPath());
    }

    private int parseInt(Properties props, String name) {
        String val = props.getProperty(name);
        if (val != null) {
            try {
                return Numbers.parseInt(val);
            } catch (NumericException e) {
                System.out.println(name + ": invalid value");
            }
        }
        return -1;
    }

    private int parseSize(Properties props, String name) {
        String val = props.getProperty(name);
        if (val != null) {
            try {
                return Numbers.parseIntSize(val);
            } catch (NumericException e) {
                System.out.println(name + ": invalid value");
            }
        }
        return -1;
    }
}
