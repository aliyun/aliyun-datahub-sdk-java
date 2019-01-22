/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.datahub.common.transport;

import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.common.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * DefaultTransport基于JDK的 HttpURLConnection}提供HTTP请求功能
 */
public class DefaultTransport implements Transport {

    private DatahubConfiguration config;

    public DefaultTransport(DatahubConfiguration config) {
        this.config = config;
    }

    @Override
    public Connection connect(DefaultRequest req) throws IOException {
        DefaultConnection conn = new DefaultConnection(this.config);
        conn.connect(req);
        return conn;
    }

    @Override
    public void close() {

    }

    @Override
    public Response request(DefaultRequest req) throws IOException {
        Connection conn = connect(req);
        DefaultResponse resp = null;
        try {
            // send request body
            if (req.getBody() != null && req.getBody().length != 0) {
                OutputStream out = conn.getOutputStream();
                IOUtils.copyLarge(new ByteArrayInputStream(req.getBody()), out);
                out.close();
            }

            resp = (DefaultResponse) conn.getResponse();

            if (HttpMethod.HEAD != req.getHttpMethod()) {
                InputStream in = conn.getInputStream();
                resp.setBody(IOUtils.readFully(in));
            }

        } finally {
            conn.disconnect();
        }
        return resp;
    }

    @Override
    public Response request(DefaultRequest req, String endpoint) throws IOException {
        return request(req);
    }

}
