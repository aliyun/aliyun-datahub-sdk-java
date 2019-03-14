package com.aliyun.datahub.common.transport;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class HttpClient {
	Logger logger = LoggerFactory.getLogger(HttpClient.class);
	/**
	 * 每个路由最大连接数
	 */
	public final static int MAX_ROUTE_CONNECTIONS = 200;
	private CloseableHttpClient httpClient;

	private int connectionTimeout = 30000;

	private int soTimeout = 15000;
    private int maxRouteConnections = MAX_ROUTE_CONNECTIONS;
	private int totalConn = MAX_ROUTE_CONNECTIONS;
	private PoolingHttpClientConnectionManager connManager;

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public int getMaxRouteConnections() {
		return maxRouteConnections;
	}

	public void setMaxRouteConnections(int maxRouteConnections) {
		this.maxRouteConnections = maxRouteConnections;
	}

	public int getTotalConn() {
		return totalConn;
	}

	public void setTotalConn(int totalConn) {
		this.totalConn = totalConn;
	}

	public void init() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		SSLContext sslContext = SSLContextBuilder
				.create()
				.loadTrustMaterial(new TrustSelfSignedStrategy())
				.build();
		final SSLSocketFactory theSslSocketFactory =
				new SSLSocketFactory(sslContext,
						SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

		connManager = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
				.register("http", new PlainConnectionSocketFactory())
				.register("https", theSslSocketFactory)
				.build());
		connManager.setDefaultMaxPerRoute(maxRouteConnections);
		connManager.setMaxTotal(totalConn);

		httpClient = HttpClientBuilder.create()
				.disableContentCompression()
				.setDefaultConnectionConfig(ConnectionConfig.custom()
						.setCharset(Charset.forName("UTF-8"))
						.build())
				.setConnectionManager(connManager)
				.setDefaultRequestConfig(RequestConfig.custom()
						.setConnectTimeout(connectionTimeout)
						.setExpectContinueEnabled(false)
						.build())
				.setDefaultSocketConfig(SocketConfig.custom()
						.setSoTimeout(soTimeout)
						.build())
				.build();
	}

	public void shutdown() {
		if (connManager != null) {
			connManager.shutdown();
		}
	}

	class HttpResultResponseHandler implements ResponseHandler<HttpResult> {

		public HttpResult handleResponse(HttpResponse response)
				throws IOException {
			// TODO Auto-generated method stub
			HttpResult result = new HttpResult();
			HttpEntity entity = response.getEntity();
			Header[] headers=response.getAllHeaders();
			if(ArrayUtils.isNotEmpty(headers)){
				HashMap<String,String> headerMap=new HashMap<String,String>();
				for(Header h:headers){
					headerMap.put(h.getName(), h.getValue());
				}
				result.setHeader(headerMap);
			}
			if (entity != null) {
				byte[] res = EntityUtils.toByteArray(entity);
				result.setBody(res);
			}
			result.setCode(response.getStatusLine().getStatusCode());

			return result;
		}
	}

	public HttpResult doPostWithBytes(String url, Map<String, String> headers, byte [] body) throws IOException {
		HttpPost httppost = null;
		try {
			httppost = new HttpPost(url);
			setHeaders(httppost, headers);
			httppost.setEntity(new ByteArrayEntity(body));
			HttpResultResponseHandler sph = new HttpResultResponseHandler();
			return httpClient.execute(httppost, sph);
		} catch (Exception e) {
			if (httppost != null) {
				httppost.abort();
			}
			throw new IOException(e);
		}
	}

	public HttpResult doPutWithBytes(String url, Map<String, String> headers, byte [] body) throws IOException {
		HttpPut httpput = null;
		try {
			httpput = new HttpPut(url);
			setHeaders(httpput, headers);
			httpput.setEntity(new ByteArrayEntity(body));
			HttpResultResponseHandler sph = new HttpResultResponseHandler();
			return httpClient.execute(httpput, sph);
		} catch (Exception e) {
			if (httpput != null) {
				httpput.abort();
			}
			throw new IOException(e);
		}
	}

	public HttpResult doGet(String url, Map<String, String> headers) throws IOException {
		HttpGet httpget = null;
		try {
			httpget = new HttpGet(url);
			setHeaders(httpget, headers);
			HttpResultResponseHandler sph = new HttpResultResponseHandler();
			return httpClient.execute(httpget, sph);
		} catch (Exception e) {
			if (httpget != null) {
				httpget.abort();
			}
			throw new IOException(e);
		}
	}

	public HttpResult doDelete(String url, Map<String, String> headers) throws IOException {
		HttpDelete httpdelete = null;
		try {
			httpdelete = new HttpDelete(url);
			setHeaders(httpdelete, headers);
			HttpResultResponseHandler sph = new HttpResultResponseHandler();
			return httpClient.execute(httpdelete, sph);
		} catch (Exception e) {
			if (httpdelete != null) {
				httpdelete.abort();
			}
			throw new IOException(e);
		}
	}

	public HttpResult doHead(String url, Map<String, String> headers) throws IOException {
		HttpHead httphead = null;
		try {
			httphead = new HttpHead(url);
			setHeaders(httphead, headers);
			HttpResultResponseHandler sph = new HttpResultResponseHandler();
			return httpClient.execute(httphead, sph);
		} catch (Exception e) {
			if (httphead != null) {
				httphead.abort();
			}
			throw new IOException(e);
		}
	}

	private void setHeaders(HttpRequestBase http, Map<String, String> headers) {
		if (headers != null) {
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				if (!entry.getKey().equals(Headers.CONTENT_LENGTH)) {
					http.addHeader(entry.getKey(), entry.getValue());
				}
			}
		}
	}

	public class HttpResult {

		private byte [] body;

		private Map<String, String> header;

		private int code;

		public byte[] getBody() {
			return body;
		}

		public void setBody(byte [] body) {
			this.body = body;
		}

		public Map<String, String> getHeader() {
			return header;
		}

		public void setHeader(Map<String, String> header) {
			this.header = header;
		}

		public int getCode() {
			return code;
		}

		public void setCode(int code) {
			this.code = code;
		}
	}
}
