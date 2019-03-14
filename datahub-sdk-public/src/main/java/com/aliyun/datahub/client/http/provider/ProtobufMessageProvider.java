package com.aliyun.datahub.client.http.provider;

import com.aliyun.datahub.client.http.provider.protobuf.BaseProtobufModel;
import com.aliyun.datahub.client.http.provider.protobuf.MessageInputStream;
import com.aliyun.datahub.client.http.provider.protobuf.MessageOutputStream;
import com.google.protobuf.Message;

import javax.ws.rs.Consumes;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Provider
@Produces("application/x-protobuf")
@Consumes("application/x-protobuf")
public class ProtobufMessageProvider implements MessageBodyReader<BaseProtobufModel>, MessageBodyWriter<BaseProtobufModel> {
    private boolean enablePbCrc;
    private final Map<Class<BaseProtobufModel>, Method> methodCache = new ConcurrentHashMap<>();

    public ProtobufMessageProvider(boolean enablePbCrc) {
        this.enablePbCrc = enablePbCrc;
    }

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return isAssignableFrom(aClass);
    }

    @Override
    public BaseProtobufModel readFrom(Class<BaseProtobufModel> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream) throws IOException, WebApplicationException {
        try {
            final MessageInputStream buffer = new MessageInputStream(inputStream, enablePbCrc);
            final Message.Builder builder = (Message.Builder) getNewBuilder(aClass).invoke(aClass);
            Message message = builder.mergeFrom(buffer).build();
            BaseProtobufModel model = aClass.newInstance();
            model.setMessage(message);
            return model;
        } catch (Exception e) {
            throw new IOException("Read from pb data fail. error:" + e.getMessage());
        }
    }

    private Method getNewBuilder(Class<BaseProtobufModel> aClass) throws NoSuchMethodException {
        Method builder = methodCache.get(aClass);
        if (builder == null) {
            builder = aClass.getMethod("newBuilder");
            methodCache.put(aClass, builder);
        }

        return builder;
    }

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return isAssignableFrom(aClass);
    }

    @Override
    public long getSize(BaseProtobufModel baseProtobufModel, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        // deprecated by JAX-RS 2.0 and ignored by Jersey runtime
        return -1;
    }

    @Override
    public void writeTo(BaseProtobufModel baseProtobufModel, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream) throws IOException, WebApplicationException {
        try {
            final MessageOutputStream buffer = new MessageOutputStream(enablePbCrc);
            // write size
            final Message message = baseProtobufModel.getMessage();
            buffer.writeInt(message.getSerializedSize());
            // write body
            message.writeTo(buffer);
            outputStream.write(buffer.toByteArray());
        } catch (Exception e) {
            throw new ProcessingException(e.getMessage());
        }
    }

    private boolean isAssignableFrom(Class<?> type) {
        return BaseProtobufModel.class.isAssignableFrom(type);
    }
}
