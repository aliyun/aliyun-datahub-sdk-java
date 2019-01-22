package com.aliyun.datahub.model.compress;

import com.aliyun.datahub.common.transport.Headers;
import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.exception.DatahubClientException;
import net.jpountz.lz4.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.*;

public class Compression {
    public static byte[] compress(byte[] data, CompressionFormat compressionFormat) {
        if (compressionFormat.equals(CompressionFormat.LZ4)) {
            return lz4Compress(data);
        } else if (compressionFormat.equals(CompressionFormat.ZLIB)) {
            try {
                return zlibCompress(data);
            } catch (IOException e) {
                throw new DatahubClientException(e.getMessage());
            }
        } else {
            throw new DatahubClientException("Unsupported compression format.");
        }
    }

    public static byte[] decompress(byte[] compressed, CompressionFormat compressionFormat) {
        if (compressionFormat.equals(CompressionFormat.LZ4)) {
            throw new DatahubClientException("raw size hint is missing.");
        } else if (compressionFormat.equals(CompressionFormat.ZLIB)) {
            try {
                return zlibDecompress(compressed);
            } catch (IOException e) {
                throw new DatahubClientException(e.getMessage());
            }
        } else {
            throw new DatahubClientException("Unsupported compression format.");
        }
    }

    public static byte[] decompress(byte[] compressed, CompressionFormat compressionFormat, int rawSizeHint) {
        if (compressionFormat.equals(CompressionFormat.LZ4)) {
            if (rawSizeHint <= 0 ) {
                throw new DatahubClientException("Raw size is invalied:" + rawSizeHint);
            }
            return lz4Decompress(compressed, Integer.valueOf(rawSizeHint));
        } else if (compressionFormat.equals(CompressionFormat.ZLIB)) {
            try {
                return zlibDecompress(compressed);
            } catch (IOException e) {
                throw new DatahubClientException(e.getMessage());
            }
        } else {
            throw new DatahubClientException("Unsupported compression format.");
        }
    }

    public static byte[] lz4Compress(byte[] dataToCompress) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        byte[] compressed = compressor.compress(dataToCompress);
        return compressed;
    }

    public static byte[] lz4Decompress(byte[] compressed, int len) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4SafeDecompressor decompressor = factory.safeDecompressor();
        byte[] restored = new byte[len];
        int size = decompressor.decompress(compressed, restored);
        restored = Arrays.copyOf(restored, size);
        return restored;
    }

    public static byte[] zlibCompress(byte[] dataToCompress) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        Deflater deflater = new Deflater(Deflater.BEST_SPEED);
        DeflaterOutputStream zlibStream = new DeflaterOutputStream(byteStream, deflater);
        zlibStream.write(dataToCompress);
        zlibStream.close();
        byteStream.close();
        byte[] compressedData = byteStream.toByteArray();
        return compressedData;
    }

    public static byte[] zlibDecompress(byte[] compressed) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        InflaterOutputStream zlibStream = new InflaterOutputStream(byteStream);
        zlibStream.write(compressed);
        zlibStream.close();
        byteStream.close();
        byte[] restored = byteStream.toByteArray();
        return restored;
    }
}
