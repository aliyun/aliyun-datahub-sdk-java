package com.aliyun.datahub.client.http.interceptor;

import com.aliyun.datahub.client.metircs.ClientMetrics;
import com.codahale.metrics.Meter;
import org.apache.commons.io.output.CountingOutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;

public class MetricInterceptor implements WriterInterceptor, ReaderInterceptor {
    private final Meter putMeter = ClientMetrics.getMeter(ClientMetrics.MetricType.PUT_TPS);
    private final Meter getMeter = ClientMetrics.getMeter(ClientMetrics.MetricType.GET_TPS);

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext readerContext) throws IOException, WebApplicationException {
        String requestLength = readerContext.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);

        if (!requestLength.equals("-1")) {
            // add metric
            if (getMeter != null) {
                getMeter.mark(Integer.parseInt(requestLength));
            }
        }
        return readerContext.proceed();
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext writerContext) throws IOException, WebApplicationException {
        CountingOutputStream outputStream
                = new CountingOutputStream(writerContext.getOutputStream());
        writerContext.setOutputStream(outputStream);
        writerContext.proceed();
        // add metric
        if (putMeter != null) {
            putMeter.mark(outputStream.getCount());
        }
    }
}
