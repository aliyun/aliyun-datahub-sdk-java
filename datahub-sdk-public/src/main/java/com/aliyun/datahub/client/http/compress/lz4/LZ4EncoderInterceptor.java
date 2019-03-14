package com.aliyun.datahub.client.http.compress.lz4;

import com.aliyun.datahub.client.http.HttpConfig;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;

public class LZ4EncoderInterceptor implements WriterInterceptor, ReaderInterceptor {
    private static final String LZ4_STR = HttpConfig.CompressType.LZ4.getValue();

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException, WebApplicationException {
        String contentEncoding = context.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING);
        if (contentEncoding != null && contentEncoding.equalsIgnoreCase(LZ4_STR)) {
            context.setInputStream(new LZ4InputStream(context));
        }
        return context.proceed();
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        String contentEncoding = (String) context.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING);
        if (contentEncoding != null && contentEncoding.equalsIgnoreCase(LZ4_STR)) {
            context.setOutputStream(new LZ4OutputStream(context));
        }
        context.proceed();
    }
}
