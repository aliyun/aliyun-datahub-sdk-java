package com.aliyun.datahub.client.http.interceptor.compress;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.http.interceptor.HttpInterceptor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class Lz4RequestInterceptor extends HttpInterceptor {
    private static final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

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
                .header(HttpHeaders.CONTENT_ENCODING, "lz4")
                .header(HttpHeaders.ACCEPT_ENCODING, "lz4")
                .header(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE, String.valueOf(wrapper.getRawSize()))
                .method(request.method(), wrapper.getBody())
                .build();
    }

    private RequestBodyWrapper compressBody(RequestBody body) throws IOException {
        Buffer buffer = new Buffer();
        body.writeTo(buffer);
        long rawSize = buffer.size();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        buffer.copyTo(out);

        byte[] compressBuffer = compressor.compress(out.toByteArray());
        return new RequestBodyWrapper(rawSize, RequestBody.create(body.contentType(), compressBuffer));
    }

    private Response decompressResponse(Response response) throws IOException {
        if (response.body() == null || response.header(HttpHeaders.CONTENT_ENCODING) == null) {
            return response;
        }

        String rawSizeStr = response.header(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE);
        if (rawSizeStr == null) {
            return response;
        }

        return response.newBuilder()
                .removeHeader(HttpHeaders.CONTENT_ENCODING)
                .body(decompressBody(Integer.valueOf(rawSizeStr),response.body()))
                .build();

    }

    private ResponseBody decompressBody(int rawSize, ResponseBody body) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(body.byteStream(),out);

        byte[] restored = new byte[rawSize];
        int size = decompressor.decompress(out.toByteArray(), restored);
        return ResponseBody.create(body.contentType(), Arrays.copyOfRange(restored, 0, size));
    }
}
