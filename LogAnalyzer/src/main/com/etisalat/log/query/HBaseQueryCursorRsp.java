package com.etisalat.log.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HBaseQueryCursorRsp {
    protected static final Logger logger = LoggerFactory.getLogger(HBaseQueryCursorRsp.class);
    private Map<String, String> resultJsonObjMap = null;

    public HBaseQueryCursorRsp(Map<String, String> resultJsonObjMap) {
        this.resultJsonObjMap = resultJsonObjMap;
    }

    public Map<String, String> getResultJsonObjMap() {
        return resultJsonObjMap;
    }

    public void setResultJsonObjMap(Map<String, String> resultJsonObjMap) {
        this.resultJsonObjMap = resultJsonObjMap;
    }
}
