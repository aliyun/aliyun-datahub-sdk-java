package com.aliyun.datahub;

import com.aliyun.datahub.model.compress.CompressionFormat;
import org.testng.annotations.Factory;

public class GetRecordsTestFactory {
    @Factory
    public Object [] createInstances() {
        return new Object[] {
                new GetRecordsTest(null),
                new GetRecordsTest(CompressionFormat.LZ4),
                new GetRecordsTest(CompressionFormat.ZLIB)
        };
    }
}
