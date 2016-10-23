package com.etisalat.log.sort;

import com.etisalat.log.config.LogConfFactory;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SortTest {
    protected static final Logger logger = LoggerFactory.getLogger(SortTest.class);

    public static void main(String[] args) {
        logger.info("-------------------------------");
        LogConfFactory.init();
        logger.info("-------------------------------");

        Map<String, List<JsonObject>> shardResults = new HashMap<String, List<JsonObject>>();
        List<SortField> fields = new ArrayList<SortField>();
        fields.add(new SortField("timestamp", true, SortField.Type.long_n));

        Map<String, JsonObject> map = new HashMap<String, JsonObject>();
        System.out.println("--------- zzzz --------");
        shardResults.put("zzzz", getJsonObjList(fields, 20, map));
        System.out.println("--------- yyyy --------");
        shardResults.put("yyyy", getJsonObjList(fields, 20, map));

        List<JsonObject> res = SortUtils.sort(shardResults, fields, 10, "rowkey");
        for (JsonObject rs : res) {
        	System.out.println(rs.toString());
        } 
    }

    private static List<JsonObject> getJsonObjList(List<SortField> fields, int size, Map<String, JsonObject> map) {
        List<JsonObject> jsonObjects = new ArrayList<JsonObject>();

        for (int i = 0; i < size; i++) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("rowkey", randomString());
            jsonObject.addProperty("timestamp", Long.valueOf(randomInteger(10)));
            jsonObjects.add(jsonObject);
            map.put((String) jsonObject.get("rowkey").getAsString(), jsonObject);
        }
        SortUtils.sortSingleShardRsp(fields, jsonObjects);

        return jsonObjects;
    }

    private static String randomInteger(int length) {
        if (length < 1) {
            return "";
        }
        Random randGen = null;
        char[] numbersAndLetters = null;
        if (randGen == null) {
            randGen = new Random();
            numbersAndLetters = ("12356789").toCharArray();
        }
        char[] randBuffer = new char[length];
        for (int i = 0; i < randBuffer.length; i++) {
            randBuffer[i] = numbersAndLetters[randGen.nextInt(numbersAndLetters.length)];
        }
        return new String(randBuffer);
    }

    private static String randomString() {
        return System.nanoTime() + "#" + randomInteger(3);
    }
}
