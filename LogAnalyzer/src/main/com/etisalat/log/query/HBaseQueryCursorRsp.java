package com.etisalat.log.query;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HBaseQueryCursorRsp {
    protected static final Logger logger = LoggerFactory.getLogger(HBaseQueryCursorRsp.class);
    private Map<String, JsonObject> resultJsonObjMap = null;

    public HBaseQueryCursorRsp(Map<String, JsonObject> resultJsonObjMap) {
        this.resultJsonObjMap = resultJsonObjMap;
    }

    public Map<String, JsonObject> getResultJsonObjMap() {
        return resultJsonObjMap;
    }

    public void setResultJsonObjMap(Map<String, JsonObject> resultJsonObjMap) {
        this.resultJsonObjMap = resultJsonObjMap;
    }
}
