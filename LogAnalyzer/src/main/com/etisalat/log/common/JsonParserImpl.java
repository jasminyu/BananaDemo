package com.etisalat.log.common;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.lang.reflect.Type;

public class JsonParserImpl implements JsonParser {
    private static final Logger logger = LoggerFactory.getLogger(JsonParserImpl.class);
    private Gson gson = new Gson();

    @Override
    public <T> T fromJson(T object, Class<T> type) {
        try {
            return gson.fromJson(gson.toJson(object, type), type);
        } catch (Exception e) {
            logger.warn("failed to get jsob object", e);
            return null;
        }
    }

    @Override
    public <T> T fromJson(String json, Class<T> tClass) {
        AssertUtil.notBlank(json, "Fail to parse json to object: blank json string.");
        AssertUtil.notNull(tClass, "Fail to parse json to object: null class parameter.");
        return gson.fromJson(json, tClass);
    }

    @Override
    public String toJson(Object object) {
        AssertUtil.notNull(object, "Fail to parse object to json string: null object.");
        return gson.toJson(object);
    }

    @Override
    public <T> T fromJson(Reader reader, Class<T> tClass) {
        return gson.fromJson(reader, tClass);
    }

    @Override
    public <T> T fromJson(T object, Type type) {
        try {
            return gson.fromJson(gson.toJson(object, type), type);
        } catch (Exception e) {
            logger.warn("failed to get jsob object", e);
            return null;
        }
    }
}
