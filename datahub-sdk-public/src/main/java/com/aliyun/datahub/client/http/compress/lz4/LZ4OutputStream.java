package com.aliyun.datahub.client.http.compress.lz4;

import com.aliyun.datahub.client.common.DatahubConstant;
import com.aliyun.datahub.client.http.HttpRequest;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

public class LZ4OutputStream extends FilterOutputStream {
    private final WriterInterceptorContext context;
    private final ByteArrayOutputStream innerBuffer;
    private final LZ4Compressor compressor;

    public LZ4OutputStream(WriterInterceptorContext context) {
        super(context.getOutputStream());
        this.context = context;
        this.innerBuffer = new ByteArrayOutputStream();
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
    }

    @Override
    public void write(int b) throws IOException {
        this.innerBuffer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.innerBuffer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.innerBuffer.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        if (innerBuffer.size() > 0) {
            String rawSizeStr = String.valueOf(innerBuffer.size());
            HttpRequest httpRequest = (HttpRequest) context.getProperty(DatahubConstant.PROP_INTER_HTTP_REQUEST);
            context.getHeaders().putSingle(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE, rawSizeStr);
            httpRequest.header(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE, rawSizeStr);

            byte[] compressedBuffer = compressor.compress(innerBuffer.toByteArray());
            out.write(compressedBuffer);
            innerBuffer.reset();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        this.innerBuffer.close();
    }
}
