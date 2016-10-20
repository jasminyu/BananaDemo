package com.etisalat.log.query;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HBaseQueryCursorRsp {
    protected static final Logger logger = LoggerFactory.getLogger(HBaseQueryCursorRsp.class);
    private List<JsonObject> resultJsonObjList = null;

    public HBaseQueryCursorRsp(List<JsonObject> resultJsonObjList) {
        this.resultJsonObjList = resultJsonObjList;
    }

    public List<JsonObject> getResultJsonObjList() {
        return resultJsonObjList;
    }

    public void setResultJsonObjList(List<JsonObject> resultJsonObjList) {
        this.resultJsonObjList = resultJsonObjList;
    }
}
