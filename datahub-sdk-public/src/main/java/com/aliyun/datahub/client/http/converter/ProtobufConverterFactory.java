package com.aliyun.datahub.client.http.converter;

import com.google.protobuf.Message;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Converter;
import retrofit2.Retrofit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufConverterFactory extends Converter.Factory {
    private boolean enablePbCrc;

    public ProtobufConverterFactory(boolean enablePbCrc) {
        this.enablePbCrc = enablePbCrc;
    }

    public static ProtobufConverterFactory create(boolean enableCrc) {
        return new ProtobufConverterFactory(enableCrc);
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public Converter<ResponseBody, ?> responseBodyConverter(Type type, Annotation[] annotations, Retrofit retrofit) {
        if (!(type instanceof Class<?>)) {
            return null;
        }
        Class<?> cls = (Class<?>) type;
        if (!isAssignableFrom(cls)) {
            return null;
        }
        return new ProtobufResponseBodyConverter<>(enablePbCrc, (Class<? extends BaseProtobufModel>) cls);
    }

    @Nullable
    @Override
    public Converter<?, RequestBody> requestBodyConverter(Type type, Annotation[] parameterAnnotations, Annotation[] methodAnnotations, Retrofit retrofit) {
        if (!(type instanceof Class<?>)) {
            return null;
        }
        Class<?> cls = (Class<?>) type;
        if (!isAssignableFrom(cls)) {
            return null;
        }
        return new ProtobufRequestBodyConverter<>(enablePbCrc);
    }

    private boolean isAssignableFrom(Class<?> type) {
        return BaseProtobufModel.class.isAssignableFrom(type);
    }

    private static class ProtobufResponseBodyConverter<T extends BaseProtobufModel> implements Converter<ResponseBody, T> {
        private final static Map<Class<?>, Method> METHOD_CACHE = new ConcurrentHashMap<>();

        private boolean enablePbCrc;
        private Class<T> cls;

        public ProtobufResponseBodyConverter(boolean enablePbCrc, Class<T> cls) {
            this.enablePbCrc = enablePbCrc;
            this.cls = cls;
        }

        @Nullable
        @Override
        public T convert(ResponseBody value) throws IOException {
            try {
                MessageInputStream buffer = new MessageInputStream(value.byteStream(), enablePbCrc);
                Message.Builder builder = (Message.Builder) getNewBuilder(cls).invoke(cls);
                Message message = builder.mergeFrom(buffer).build();
                T model = cls.newInstance();
                model.setMessage(message);
                return model;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                value.close();
            }
        }

        private Method getNewBuilder(Class<T> aClass) throws NoSuchMethodException {
            Method builder = METHOD_CACHE.get(aClass);
            if (builder == null) {
                builder = aClass.getMethod("newBuilder");
                METHOD_CACHE.put(aClass, builder);
            }
            return builder;
        }
    }

    private static class ProtobufRequestBodyConverter<T extends BaseProtobufModel> implements Converter<T, RequestBody> {
        private static final MediaType MEDIA_TYPE = MediaType.get("application/x-protobuf");
        private boolean enablePbCrc;

        public ProtobufRequestBodyConverter(boolean enablePbCrc) {
            this.enablePbCrc = enablePbCrc;
        }

        @Nullable
        @Override
        public RequestBody convert(T value) throws IOException {
            MessageOutputStream buffer = new MessageOutputStream(enablePbCrc);
            Message message = value.getMessage();
            buffer.writeInt(message.getSerializedSize());
            message.writeTo(buffer);
            return RequestBody.create(MEDIA_TYPE, buffer.toByteArray());
        }
    }
}