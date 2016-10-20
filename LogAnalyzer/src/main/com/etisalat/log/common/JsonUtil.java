package com.etisalat.log.common;

import java.io.Reader;
import java.lang.reflect.Type;

public class JsonUtil {
    private static JsonParser jsonParser;

    public synchronized static void setJsonParser(JsonParser jsonParser) {

        if (JsonUtil.jsonParser != null) {
            return;
        }
        JsonUtil.jsonParser = jsonParser;
    }

    public static <T> T fromJson(String json, Class<T> tClass) {
        return jsonParser.fromJson(json, tClass);
    }

    public static <T> T fromJson(T json, Class<T> tClass) {
        return jsonParser.fromJson(json, tClass);
    }

    public static <T> T fromJson(T json, Type type) {
        return jsonParser.fromJson(json, type);
    }

    public static <T> T fromJson(Reader reader, Class<T> tClass) {
        return jsonParser.fromJson(reader, tClass);
    }

    public static String toJson(Object object) {
        return jsonParser.toJson(object);
    }
}
