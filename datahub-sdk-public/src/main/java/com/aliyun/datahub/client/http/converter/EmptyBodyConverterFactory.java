package com.aliyun.datahub.client.http.converter;

import com.aliyun.datahub.client.model.BaseResult;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Converter;
import retrofit2.Retrofit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmptyBodyConverterFactory extends Converter.Factory {

    public static EmptyBodyConverterFactory create() {
        return new EmptyBodyConverterFactory();
    }

    @Nullable
    @Override
    public Converter<ResponseBody, ?> responseBodyConverter(Type type, Annotation[] annotations, Retrofit retrofit) {
        if (!(type instanceof Class<?>)) {
            return null;
        }

        Class<?> cls = (Class<?>) type;
        if (BaseResult.class.isAssignableFrom(cls)) {
            List<Field> fields = getAllFields(cls);
            if (fields.size() == 1 && "requestId".equalsIgnoreCase(fields.get(0).getName())) {
                return new EmptyBodyResponseBodyConverter<>(cls);
            }
        }
        return null;
    }

    @Nullable
    @Override
    public Converter<?, RequestBody> requestBodyConverter(Type type, Annotation[] parameterAnnotations, Annotation[] methodAnnotations, Retrofit retrofit) {
        return null;
    }

    private List<Field> getAllFields(Class cls) {
        List<Field> fields = new ArrayList<>() ;
        while (cls != null) {
            fields.addAll(Arrays.asList(cls.getDeclaredFields()));
            cls = cls.getSuperclass();
        }
        return fields;
    }

    private static class EmptyBodyResponseBodyConverter<T> implements Converter<ResponseBody, T> {
        private Class<T> retCls;

        EmptyBodyResponseBodyConverter(Class<T> cls) {
            this.retCls = cls;
        }

        @Nullable
        @Override
        public T convert(ResponseBody value) throws IOException {
            value.close();
            try {
                return retCls.newInstance();
            } catch (Exception e) {
                return null;
            }
        }
    }
}
