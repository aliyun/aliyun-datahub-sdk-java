package com.aliyun.datahub.client.auth;

import com.aliyun.datahub.client.common.DatahubConstant;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.core.HttpHeaders;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class Authorization {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String DEFAULT_HASH = "HmacSHA1";

    static public String getAkAuthorization(Request request) {
        String canonicalURL = getCanonicalURI(request.getUrlPath());
        String canonicalQueryString = getCanonicalQueryString(request.getQueryStrings());
        String canonicalHeaderString = getCanonicalHeaders(
                getSortedHeadersToSign(request.getHeaders()));

        String canonicalRequest = request.getMethod().toUpperCase() + "\n" +
                canonicalHeaderString + "\n" +
                canonicalURL + (canonicalQueryString.length() > 0 ? "?" + canonicalQueryString : "");

        String signature = HMAC1Sign(request.getAccessKey(), canonicalRequest);

        return "DATAHUB " + request.getAccessId() + ":" + signature;
    }

    static private String HMAC1Sign(String accessKey, String canonicalRequest) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(accessKey.getBytes(), DEFAULT_HASH);
            Mac mac = Mac.getInstance(DEFAULT_HASH);
            mac.init(signingKey);
            return Base64.encodeBase64String(
                    mac.doFinal(canonicalRequest.getBytes(DEFAULT_ENCODING))
            ).trim();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    static private String getCanonicalHeaders(Map<String, String> headers) {
        StringBuilder sb = new StringBuilder("");
        Iterator<Map.Entry<String, String>> pairs = headers.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            if (pair.getKey().startsWith(DatahubConstant.X_DATAHUB_PREFIX)) {
                sb.append(pair.getKey());
                sb.append(":");
                sb.append(pair.getValue());
            } else {
                sb.append(pair.getValue());
            }

            if (pairs.hasNext()) {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    static private String getCanonicalURI(String urlPath) {
        // nothing ?
        return urlPath;
    }

    static private String getCanonicalQueryString(Map<String, String> queryStrings) {
        SortedMap<String, String> sortedQuery = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : queryStrings.entrySet()) {
            sortedQuery.put(entry.getKey(), entry.getValue());
        }

        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> pairs = sortedQuery.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            sb.append(pair.getKey());
            if (pair.getValue() != null && pair.getValue().length() > 0) {
                sb.append("=");
                sb.append(pair.getValue());
            }

            if (pairs.hasNext()) {
                sb.append("&");
            }
        }
        return sb.toString();
    }

    static private SortedMap<String,String> getSortedHeadersToSign(Map<String, String> headers) {
        SortedMap<String, String> sortedHeaders = new TreeMap<String, String>();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String lowerKey = entry.getKey().toLowerCase();
            if (lowerKey.equalsIgnoreCase(HttpHeaders.CONTENT_TYPE) ||
                    lowerKey.equalsIgnoreCase(HttpHeaders.DATE) ||
                    lowerKey.startsWith(DatahubConstant.X_DATAHUB_PREFIX)) {
                sortedHeaders.put(lowerKey, entry.getValue());
            }
        }

        if (!sortedHeaders.containsKey(HttpHeaders.CONTENT_TYPE.toLowerCase())) {
            sortedHeaders.put(HttpHeaders.CONTENT_TYPE.toLowerCase(), "");
        }

        return sortedHeaders;
    }

    public static class Request {
        private String accessId;
        private String accessKey;
        private String urlPath;
        private String method;
        private Map<String, String> headers;
        private Map<String, String> queryStrings;

        public String getAccessId() {
            return accessId;
        }

        public Request setAccessId(String accessId) {
            this.accessId = accessId;
            return this;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public Request setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public String getUrlPath() {
            return urlPath;
        }

        public Request setUrlPath(String urlPath) {
            this.urlPath = urlPath;
            return this;
        }

        public String getMethod() {
            return method;
        }

        public Request setMethod(String method) {
            this.method = method;
            return this;
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public Request setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Map<String, String> getQueryStrings() {
            return queryStrings;
        }

        public Request setQueryStrings(Map<String, String> queryStrings) {
            this.queryStrings = queryStrings;
            return this;
        }
    }
}