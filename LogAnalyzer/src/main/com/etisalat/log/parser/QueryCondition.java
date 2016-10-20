package com.etisalat.log.parser;

import com.etisalat.log.common.WTType;
import com.etisalat.log.sort.SortField;

import java.util.ArrayList;
import java.util.List;

public class QueryCondition {
    private String queryString;
    private String oriQString;
    private boolean uniqueKeySort;
    private boolean sort = false;
    private List<SortField> sortedFields;
    private String startTime;
    private String endTime;
    private long querySpan;
    private List<String> collections = new ArrayList<String>();
    private String errorMsg;
    private int msgCode;
    private WTType wtType;
    private int totalNum;
    private boolean isNeedUseCursor;
    private String nextCursorMark;
    private boolean exportOp;
    private boolean queryInCloudMode;
    private int totalReturnNum;

    private Cursor nextCursor;

    private String localPath;
    private String dir;

    public String getQueryString() {
        return this.queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getOriQString() {
        return this.oriQString;
    }

    public void setOriQString(String oriQString) {
        this.oriQString = oriQString;
    }

    public boolean isUniqueKeySort() {
        return uniqueKeySort;
    }

    public void setUniqueKeySort(boolean uniqueKeySort) {
        this.uniqueKeySort = uniqueKeySort;
    }

    public boolean isSort() {
        return sort;
    }

    public void setSort(boolean sort) {
        this.sort = sort;
    }

    public List<SortField> getSortedFields() {
        return sortedFields;
    }

    public void setSortedFields(List<SortField> sortedFields) {
        this.sortedFields = sortedFields;
    }

    public String getStartTime() {
        return this.startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return this.endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public long getQuerySpan() {
        return this.querySpan;
    }

    public void setQuerySpan(long querySpan) {
        this.querySpan = querySpan;
    }

    public List<String> getCollections() {
        return this.collections;
    }

    public void setCollections(List<String> collections) {
        this.collections.addAll(collections);
    }

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public int getMsgCode() {
        return this.msgCode;
    }

    public void setMsgCode(int msgCode) {
        this.msgCode = msgCode;
    }

    public WTType getWtType() {
        return this.wtType;
    }

    public void setWtType(WTType wtType) {
        this.wtType = wtType;
    }

    public int getTotalNum() {
        return this.totalNum;
    }

    public void setTotalNum(int totalNum) {
        this.totalNum = totalNum;
    }

    public boolean isNeedUseCursor() {
        return isNeedUseCursor;
    }

    public void setNeedUseCursor(boolean isNeedUseCursor) {
        this.isNeedUseCursor = isNeedUseCursor;
    }

    public String getNextCursorMark() {
        return nextCursorMark;
    }

    public void setNextCursorMark(String nextCursorMark) {
        this.nextCursorMark = nextCursorMark;
    }

    public int getTotalReturnNum() {
        return totalReturnNum;
    }

    public void setTotalReturnNum(int totalReturnNum) {
        this.totalReturnNum = totalReturnNum;
    }

    public boolean isExportOp() {
        return exportOp;
    }

    public void setExportOp(boolean exportOp) {
        this.exportOp = exportOp;
    }

    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public Cursor getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(Cursor nextCursor) {
        this.nextCursor = nextCursor;
    }

    public boolean queryInCloudMode() {
        return queryInCloudMode;
    }

    public void setQueryInCloudMode(boolean queryInCloudMode) {
        this.queryInCloudMode = queryInCloudMode;
    }

    public String toString() {
        StringBuilder objSB = new StringBuilder();
        objSB.append("\r\nqueryString : " + queryString + "\r\n").
                append("startTime : " + startTime + "\r\n").append("endTime : " + endTime + "\r\n")
                .append("querySpan : " + querySpan + "\r\n").append("collections : " + collections.toString() + "\r\n")
                .append("errorMsg : " + errorMsg + "\r\n").append("msgCode : " + msgCode + "\r\n")
                .append("wtType : " + wtType + "\r\n").append("exportOp : " + exportOp + "\r\n")
                .append("localPath : " + localPath + "\r\n").append("isNeedUseCursor : " + isNeedUseCursor + "\r\n")
                .append("nextCursorMark : " + nextCursorMark + "\r\n")
                .append("uniqueKeySort : " + uniqueKeySort + "\r\n").append("sort : " + sort + "\r\n")
                .append("totalNum : " + totalNum + "\r\n").append("totalReturnNum : " + totalReturnNum + "\r\n")
                .append("nextCursor : " + nextCursor);

        return objSB.toString();
    }
}
