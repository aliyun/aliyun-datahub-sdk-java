package com.aliyun.datahub.client.http.interceptor.compress;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.http.interceptor.HttpInterceptor;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class DeflateRequestInterceptor extends HttpInterceptor {
    @Override
    public Response intercept(Chain chain) throws IOException {
        Response response = chain.proceed(compressRequest(chain.request()));
        return decompressResponse(response);
    }

    private Request compressRequest(Request request) throws IOException {
        if (request.body() == null || request.header(HttpHeaders.CONTENT_ENCODING) != null) {
            return request;
        }

        RequestBodyWrapper wrapper = compressBody(request.body());
        return request.newBuilder()
                .header(HttpHeaders.CONTENT_ENCODING, "deflate")
                .header(HttpHeaders.ACCEPT_ENCODING, "deflate")
                .header(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE, String.valueOf(wrapper.getRawSize()))
                .method(request.method(), wrapper.getBody())
                .build();
    }

    private RequestBodyWrapper compressBody(RequestBody body) throws IOException {
        Buffer buffer = new Buffer();
        body.writeTo(buffer);
        long rawSize = buffer.size();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DeflaterOutputStream dos = new DeflaterOutputStream(out);
        buffer.copyTo(dos);
        dos.close();

        return new RequestBodyWrapper(rawSize, RequestBody.create(body.contentType(), out.toByteArray()));
    }

    private Response decompressResponse(Response response) throws IOException {
        if (response.body() == null || response.header(HttpHeaders.CONTENT_ENCODING) == null) {
            return response;
        }
        return response.newBuilder()
                .removeHeader(HttpHeaders.CONTENT_ENCODING)
                .body(decompressBody(response.body()))
                .build();

    }

    private ResponseBody decompressBody(ResponseBody body) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        InflaterInputStream iis = new InflaterInputStream(body.byteStream());
        IOUtils.copy(iis, out);

        return ResponseBody.create(body.contentType(), out.toByteArray());
    }
}
