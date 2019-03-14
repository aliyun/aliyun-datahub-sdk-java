package com.aliyun.datahub.client.http;

public class Entity<T> {
    private T entity;
    private String contentType;

    private Entity(T entity, String contentType) {
        this.entity = entity;
        this.contentType = contentType;
    }

    public static <T> Entity<T> json(T entity) {
        return new Entity<T>(entity, "application/json");
    }

    public static <T> Entity<T> protobuf(T entity) {
        return new Entity<T>(entity, "application/x-protobuf");
    }

    public T getEntity() {
        return entity;
    }

    public String getContentType() {
        return contentType;
    }
}