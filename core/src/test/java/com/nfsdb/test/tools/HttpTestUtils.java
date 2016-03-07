package com.nfsdb.test.tools;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class HttpTestUtils {
    public static HttpClientBuilder clientBuilder(boolean ssl) throws Exception {
        return (ssl ? createHttpClient_AcceptsUntrustedCerts() : HttpClientBuilder.create());
    }

    public static void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int l;
        while ((l = is.read(buf)) > 0) {
            os.write(buf, 0, l);
        }
    }

    public static void download(HttpClientBuilder b, String url, File out) throws IOException {
        try (
                CloseableHttpClient client = b.build();
                CloseableHttpResponse r = client.execute(new HttpGet(url));
                FileOutputStream fos = new FileOutputStream(out)
        ) {
            copy(r.getEntity().getContent(), fos);
        }
    }

    private static HttpClientBuilder createHttpClient_AcceptsUntrustedCerts() throws Exception {
        HttpClientBuilder b = HttpClientBuilder.create();

        // setup a Trust Strategy that allows all certificates.
        //
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                return true;
            }
        }).build();

        b.setSSLContext(sslContext);

        // here's the special part:
        //      -- need to create an SSL Socket Factory, to use our weakened "trust strategy";
        //      -- and create a Registry, to register it.
        //
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslSocketFactory)
                .build();

        // now, we create connection-manager using our Registry.
        //      -- allows multi-threaded use
        b.setConnectionManager(new PoolingHttpClientConnectionManager(socketFactoryRegistry));

        return b;
    }

    private static int upload(String resource, String url) throws IOException {
        return upload(resourceFile(resource), url);
    }

    private static File resourceFile(String resource) {
        return new File(HttpTestUtils.class.getResource(resource).getFile());
    }

    private static int upload(File file, String url) throws IOException {
        HttpPost post = new HttpPost(url);
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            MultipartEntityBuilder b = MultipartEntityBuilder.create();
            b.addPart("data", new FileBody(file));
            post.setEntity(b.build());
            HttpResponse r = client.execute(post);
            return r.getStatusLine().getStatusCode();
        }
    }

    private static Header findHeader(String name, Header[] headers) {
        for (Header h : headers) {
            if (name.equals(h.getName())) {
                return h;
            }
        }

        return null;
    }

}
