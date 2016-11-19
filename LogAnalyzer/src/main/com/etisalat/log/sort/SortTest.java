package com.etisalat.log.sort;

import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.query.ResultCnt;
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
        testSort();
    }

    public static void print(Map<ResultCnt, String> reqUrlMap) {
        Set<Map.Entry<ResultCnt, String>> entrySet = reqUrlMap.entrySet();
        int i = 1;
        for (Map.Entry<ResultCnt, String> entry : entrySet) {
            logger.warn("xxx {}, url: {}, numFound: {}", i++, entry.getValue(), entry.getKey().toString());
        }
    }

    private static void testSortResultFound() {
        Map<ResultCnt, String> reqUrlMap = new TreeMap<ResultCnt, String>(new Comparator<ResultCnt>() {
            @Override
            public int compare(ResultCnt o1, ResultCnt o2) {
                return o1.compareTo(o2);
            }
        });

        reqUrlMap.put(new ResultCnt("tb_20101021_shard23", 12), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101021_shard4", 12), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard34", 23), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard15", 62), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101021_shard43", 23), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101021_shard2", 10), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard2", 12), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard3", 23), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard12", 32), "xxx");
        reqUrlMap.put(new ResultCnt("tb_20101022_shard15", 62), "xxx");

        Set<ResultCnt> set = reqUrlMap.keySet();
        for (ResultCnt resultCnt : set) {
            System.out.println(resultCnt);
        }
    }

    private static void testSort() {
        Map<String, List<String>> shardResults = new HashMap<String, List<String>>();
        List<SortField> fields = new ArrayList<SortField>();
        fields.add(new SortField("timestamp", true, SortField.Type.long_n));

        Map<String, String> map = new HashMap<String, String>();
        System.out.println("--------- zzzz --------");
        shardResults.put("zzzz", getJsonObjList(fields, 20, map));
        System.out.println("--------- yyyy --------");
        shardResults.put("yyyy", getJsonObjList(fields, 20, map));

        List<JsonObject> res = SortUtils.sort(shardResults, fields, 10, "rowkey");
        for (JsonObject rs : res) {
            System.out.println(rs.toString());
        }
    }

    private static List<String> getJsonObjList(List<SortField> fields, int size, Map<String, String> map) {
        List<String> jsonObjects = new ArrayList<String>();

        for (int i = 0; i < size; i++) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("rowkey", randomString());
            jsonObject.addProperty("timestamp", Long.valueOf(randomInteger(10)));
            jsonObjects.add(JsonUtil.toJson(jsonObject));
            map.put((String) jsonObject.get("rowkey").getAsString(), JsonUtil.toJson(jsonObject));
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
