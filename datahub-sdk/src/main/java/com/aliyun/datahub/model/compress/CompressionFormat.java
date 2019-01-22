package com.aliyun.datahub.model.compress;

public enum CompressionFormat {
    ZLIB("deflate"),
    LZ4("lz4");

    private String value;

    CompressionFormat(String value) {
        this.value = value;
    }

    public String toString() {
        return this.value;
    }

    public static CompressionFormat fromValue(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Value cannot be null or empty!");
        } else if (value.equals("deflate")) {
            return ZLIB;
        } else if (value.equals("lz4")) {
            return LZ4;
        } else {
            throw new IllegalArgumentException("Unsupported compression format" + value);
        }
    }
}
