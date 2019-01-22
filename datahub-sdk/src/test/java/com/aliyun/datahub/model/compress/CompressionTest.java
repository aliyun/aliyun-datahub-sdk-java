package com.aliyun.datahub.model.compress;

import com.aliyun.datahub.common.transport.DefaultResponse;
import com.aliyun.datahub.common.transport.Headers;
import com.aliyun.datahub.common.transport.Response;
import net.jpountz.lz4.LZ4Exception;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Random;

@Test
public class CompressionTest {

    @Test
    public void testLZ4() {
        byte[] body = new byte[100];
        new Random().nextBytes(body);
        byte[] compressed = Compression.lz4Compress(body);
        byte[] restored = Compression.lz4Decompress(compressed, body.length);
        Assert.assertArrayEquals(restored, body);
    }

    @Test
    public void testZLIB() {
        byte[] body = new byte[100];
        new Random().nextBytes(body);
        try {
            byte[] compressed = Compression.zlibCompress(body);
            byte[] restored = Compression.zlibDecompress(compressed);
            Assert.assertArrayEquals(restored, body);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLZ4Normal() {
        byte[] body = new byte[100];
        new Random().nextBytes(body);

        byte[] compressed = Compression.compress(body, CompressionFormat.LZ4);

        byte[] restored = Compression.decompress(compressed, CompressionFormat.LZ4, body.length);

        Assert.assertArrayEquals(restored, body);
    }

    @Test
    public void testZLIBNormal() {
        byte[] body = new byte[100];
        new Random().nextBytes(body);

        byte[] compressed = Compression.compress(body, CompressionFormat.ZLIB);

        byte[] restored = Compression.decompress(compressed, CompressionFormat.ZLIB);

        Assert.assertArrayEquals(restored, body);
    }
}
