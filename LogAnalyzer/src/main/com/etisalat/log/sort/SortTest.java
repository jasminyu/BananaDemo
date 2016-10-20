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
        //        JsonObject jsonObject = new JsonObject();
        //        jsonObject.addProperty("rowkey", randomString());
        //        //        jsonObject.put("timestamp", Long.valueOf("1638868981"));
        //        jsonObject.addProperty("timestamp", 1638868981l);
        //        jsonObject.addProperty("timestamp", 0.6d);
        //        System.out.println(jsonObject.get("timestamp").getClass().getName());
        //        jsonObject.addProperty("timestamp", 0.5f);
        //        System.out.println(jsonObject.get("timestamp").getClass().getName());
        //
        //                //
        //                List<SortField> fields = new ArrayList<SortField>();
        //                fields.add(new SortField("timestamp", false, SortField.Type.STRING));
        //                TreeMap<JsonObject, String> treeMap = new TreeMap<JsonObject, String>(new JSONObjComparator(fields));
        //                getJsonObjMAP(30, treeMap);
        //                Set<Map.Entry<JsonObject, String>> entrySet = treeMap.entrySet();
        //
        //                for (Map.Entry<JsonObject, String> entry : entrySet) {
        //                    System.out.println(entry.getKey().toString() + "   " + entry.getValue());
        //                }

        Map<String, List<JsonObject>> shardResults = new HashMap<String, List<JsonObject>>();
        List<SortField> fields = new ArrayList<SortField>();
        //                fields.add(new SortField("timestamp", true, SortField.Type.LONG));
        //        fields.add(new SortField("timestamp", true, SortField.Type.LONG));
        fields.add(new SortField("timestamp", true, SortField.Type.long_n));

        Map<String, JsonObject> map = new HashMap<String, JsonObject>();
        System.out.println("--------- zzzz --------");
        shardResults.put("zzzz", getJsonObjList(fields, 20, map));
        System.out.println("--------- yyyy --------");
        shardResults.put("yyyy", getJsonObjList(fields, 20, map));

        String[] res = SortUtils.sort(shardResults, fields, 10, "rowkey");

        //        for (String key : map.keySet()) {
        //            System.out.println(key);
        //        }
        JsonObject tmp = null;
        StringBuilder builder = new StringBuilder();

        for (String rs : res) {
            tmp = map.get(rs);
            if (tmp == null) {
                builder.append(tmp).append("\n");
            } else {
                System.out.println(tmp.toString());
            }

        }
        //        System.out.println(
        //                "---------------------------------- missing keys ------------------------------------------------------------------");
        //        System.out.println(builder.toString());
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

    public static String randomInteger(int length) {
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

    public static String randomString() {
        return System.nanoTime() + "#" + randomInteger(3);
    }

    public static String randomString(int length) {
        if (length < 1) {
            return "";
        }
        Random randGen = null;
        char[] numbersAndLetters = null;
        if (randGen == null) {
            randGen = new Random();
            numbersAndLetters = ("abcdefghijklmnopqrstuvwxyz012356789").toCharArray();
        }
        char[] randBuffer = new char[length];
        for (int i = 0; i < randBuffer.length; i++) {
            randBuffer[i] = numbersAndLetters[randGen.nextInt(numbersAndLetters.length)];
        }
        return new String(randBuffer);
    }
}
