package com.aliyun.datahub;

import com.aliyun.datahub.model.compress.CompressionFormat;
import org.testng.annotations.Factory;

public class PutRecordsTestFacotry {
    @Factory
    public Object [] createInstances() {
        return new Object[] {
                new PutRecordsTest(null),
                new PutRecordsTest(CompressionFormat.LZ4),
                new PutRecordsTest(CompressionFormat.ZLIB)
        };
    }
}
