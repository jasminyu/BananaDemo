package com.etisalat.log.common;

import java.io.Reader;
import java.lang.reflect.Type;

public interface JsonParser {
    <T> T fromJson(T object, Class<T> type);

    <T> T fromJson(T object, Type type);

    <T> T fromJson(String json, Class<T> tClass);

    <T> T fromJson(Reader reader, Class<T> tClass);

    String toJson(Object object);
}
