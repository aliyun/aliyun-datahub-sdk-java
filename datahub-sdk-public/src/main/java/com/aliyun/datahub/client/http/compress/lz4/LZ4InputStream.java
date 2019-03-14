package com.aliyun.datahub.client.http.compress.lz4;

import com.aliyun.datahub.client.common.DatahubConstant;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import javax.ws.rs.ext.ReaderInterceptorContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.Arrays;

public class LZ4InputStream extends FilterInputStream {
    private final ReaderInterceptorContext context;
    private final LZ4SafeDecompressor decompressor;
    private ByteArrayInputStream innerBuffer;

    public LZ4InputStream(ReaderInterceptorContext context) throws IOException {
        super(context.getInputStream());
        this.context = context;
        this.decompressor = LZ4Factory.fastestInstance().safeDecompressor();
        decompressStream();
    }

    private void decompressStream() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int len;
        byte[] buffer = new byte[1024];
        while ((len = in.read(buffer)) > -1 ) {
            baos.write(buffer, 0, len);
        }
        baos.flush();
        baos.close();

        if (!context.getHeaders().containsKey(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE)) {
            throw new IOException("Response not contains raw-size header");
        }

        int rawSize = Integer.parseInt(context.getHeaders().getFirst(DatahubConstant.X_DATAHUB_CONTENT_RAW_SIZE));
        byte[] restored = new byte[rawSize];
        int size = decompressor.decompress(baos.toByteArray(), restored);
        this.innerBuffer = new ByteArrayInputStream(Arrays.copyOf(restored, size));
    }


    @Override
    public int read() throws IOException {
        return this.innerBuffer.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.innerBuffer.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.innerBuffer.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.innerBuffer.skip(n);
    }

    @Override
    public void close() throws IOException {
        this.innerBuffer.close();
    }
}
