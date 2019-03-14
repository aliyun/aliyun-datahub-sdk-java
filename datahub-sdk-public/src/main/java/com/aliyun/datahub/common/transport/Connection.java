package com.aliyun.datahub.common.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Connection表示一次HTTP请求所使用的连接
 * 
 * 一个Connection对象与一次HTTP请求的生命周期相同, 当请求结束后Connection不再可用。
 * 
 * 一次HTTP请求有如下过程: 
 * 1. 通过 connect( DefaultRequest req)}方法发起HTTP请求, HTTP头会发送
 * 2. 通过 getOutputStream()}获得输出流，发送HTTP body数据 
 * 3.  getResponse()}获得HTTP响应，包含状态吗, HTTP头 
 * 4.  getInputStream()}读取HTTP响应的body部分数据 
 * 5. disconnect()}请求结束后释放资源
 * 
 */
public interface Connection {

    /**
     * 发起HTTP请求
     *
     * @param req HTTP请求
     * @throws IOException
     */
    public void connect(DefaultRequest req) throws IOException;

    /**
     * 获得HTTP连接上的流,以写入HTTP请求的body
     *
     * @return OutputStream
     * @throws IOException
     */
    public OutputStream getOutputStream() throws IOException;

    /**
     * 获得HTTP请求的响应
     *
     * @return Response对象
     * @throws IOException
     */
    public Response getResponse() throws IOException;

    /**
     * 获得HTTP连接上的流,以读取HTTP请求响应的body部分数据
     *
     * @return InputStream
     * @throws IOException
     */
    public InputStream getInputStream() throws IOException;

    /**
     * 关闭HTTP连接
     *
     * @throws IOException
     */
    public void disconnect() throws IOException;
}
