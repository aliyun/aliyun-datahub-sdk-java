package com.aliyun.datahub.client.http.compress.lz4;

import com.aliyun.datahub.client.http.HttpConfig;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;

public class LZ4EncoderFilter implements ClientRequestFilter {
    private static final String LZ4_STR = HttpConfig.CompressType.LZ4.getValue();

    @Override
    public void filter(ClientRequestContext request) throws IOException {
        request.getHeaders().putSingle(HttpHeaders.ACCEPT_ENCODING, LZ4_STR);
        if (request.hasEntity()) {
            if (request.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING) == null) {
                request.getHeaders().putSingle(HttpHeaders.CONTENT_ENCODING, LZ4_STR);
            }
        }
    }
}