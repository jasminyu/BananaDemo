package com.etisalat.log.parser;

import com.etisalat.log.config.LogConfFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Cursor {
    private static final String SEPARATOR = "#";
    private static Logger logger = LoggerFactory.getLogger(Cursor.class);
    private String cacheKey;
    private String collection;
    private String shardId;
    private int fetchIdx;

    public Cursor(String cacheKey, String collection, String shardId, int fetchIdx) {
        this.cacheKey = cacheKey;
        this.collection = collection;
        this.shardId = shardId;
        this.fetchIdx = fetchIdx;
    }

    public static Cursor parse(String cursorStr) throws UnsupportedEncodingException {
        if (StringUtils.isBlank(cursorStr)) {
            String errMsg = "cursor is blank.";
            logger.info(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        String[] arr = URLDecoder.decode(cursorStr, "UTF-8").split(SEPARATOR);
        if (arr.length != 3) {
            String errMsg = "cursor " + cursorStr + " should like \"part1#part2#part3\"";
            logger.info(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        logger.debug("cursor is {}", cursorStr);

        try {
            return new Cursor(arr[0], arr[1].substring(0, LogConfFactory.collectionNameLen),
                    arr[1].substring(LogConfFactory.collectionNameLen), Integer.valueOf(arr[2]));
        } catch (RuntimeException e) {
            String errMsg = "cursor " + cursorStr + " should like \"part1#part2#part3\", and the last part is numeric.";
            logger.info(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
    }

    public Cursor deepCopy() {
        return new Cursor(this.cacheKey, this.collection, this.shardId, this.fetchIdx);
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getFetchIdx() {
        return fetchIdx;
    }

    public void setFetchIdx(int fetchIdx) {
        this.fetchIdx = fetchIdx;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public String toString() {
        return cacheKey + SEPARATOR + (collection + shardId) + SEPARATOR + fetchIdx;
    }
}
