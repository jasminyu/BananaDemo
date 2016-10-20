package com.etisalat.log.sort;

import com.etisalat.log.common.AssertUtil;
import com.etisalat.log.config.LogConfFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.etisalat.log.sort.SortField.Type.text_general;

public class SortUtils {
    private static final Logger logger = LoggerFactory.getLogger(SortUtils.class);
    private static final StringComparator STRING_COMPARATOR = new StringComparator();

    public static void main(String[] args) throws IOException {
        //        Schema schema = schemaFileToBean();
        //
        //        for (Field field : schema.getFields()) {
        //            System.out.println(field.getName() + "--" + field.getType());
        //        }

        String res = LogConfFactory.getString("xxx",
                "{\"responseHeader\":{\"status\":0,\"QTime\":0,\"params\":{\"q\":\"*:*\",\"rows\":\"0\",\"wt\":\"json\"}},\"response\":{\"numFound\":0,\"start\":0,\"docs\":[]}}");

        System.out.println("xxx");
    }

    private static boolean isNumeric(SortField sortField) {
        switch (sortField.getType()) {
        case double_n:
        case float_n:
        case integer:
        case long_n:
            return true;
        default:
            return false;
        }
    }

    private static boolean isComparable(SortField sortField) {
        if (sortField.getType().equals(text_general)) {
            return false;
        }

        return true;
    }

    public static int compare(SortField sortField, JsonElement first, JsonElement second) {
        if (!isComparable(sortField)) {
            throw new RuntimeException("The type " + sortField.getType() + " does not support sort");
        }

        if (first == null) {
            if (second == null) {
                return 0;
            } else {
                return sortField.isReverse() ? 1 : -1;
            }
        }

        if (second == null) {
            return sortField.isReverse() ? -1 : 1;
        }

        switch (sortField.getType()) {
        case double_n:
            return sortField.isReverse() ?
                    -Double.compare(first.getAsDouble(), second.getAsDouble()) :
                    Double.compare(first.getAsDouble(), second.getAsDouble());
        case float_n:
            return sortField.isReverse() ?
                    -Float.compare(first.getAsFloat(), second.getAsFloat()) :
                    Float.compare(first.getAsFloat(), second.getAsFloat());
        case integer:
            return sortField.isReverse() ?
                    -Integer.compare(first.getAsInt(), second.getAsInt()) :
                    Integer.compare(first.getAsInt(), second.getAsInt());
        case long_n:
            return sortField.isReverse() ?
                    -Long.compare(first.getAsLong(), second.getAsLong()) :
                    Long.compare(first.getAsLong(), second.getAsLong());
        case tdate:
        case string:
            return sortField.isReverse() ?
                    -STRING_COMPARATOR.compare(new BytesRef(first.getAsString()), new BytesRef(second.getAsString())) :
                    STRING_COMPARATOR.compare(new BytesRef(first.getAsString()), new BytesRef(second.getAsString()));
        default:
            throw new RuntimeException("The type " + sortField.getType() + " does not support sort");
        }

    }

    public static void sortSingleShardRsp(List<SortField> fields, List<JsonObject> data) {
        AssertUtil.noneNull("invalid arguments", fields, data);
        if (fields.size() == 0 || data.size() == 0) {
            return;
        }
        Collections.sort(data, new Comparator<JsonObject>() {
            public int compare(JsonObject o1, JsonObject o2) {
                int ret = 0;
                SortField sortField = null;
                for (int i = 0; i < fields.size() && ret == 0; i++) {
                    sortField = fields.get(i);
                    ret = SortUtils.compare(sortField, o1.get(sortField.getName()), o2.get(sortField.getName()));
                }
                return ret;
            }
        });
    }

    public static JsonArray sortSingleShardRsp(List<SortField> fields, JsonArray jsonArray) {
        AssertUtil.noneNull("invalid arguments", fields, jsonArray);
        List<JsonObject> data = new ArrayList<JsonObject>();
        for (JsonElement jsonElement : jsonArray) {
            data.add(jsonElement.getAsJsonObject());
        }
        sortSingleShardRsp(fields, data);

        JsonArray resArr = new JsonArray();
        for (JsonObject jsonElement : data) {
            resArr.add(jsonElement);
        }

        return resArr;
    }

    public static String[] sort(Map<String, List<JsonObject>> shardResults, List<SortField> fields, int size,
            String uniqueKey) {
        // TODO YU check the params
        if (fields == null || fields.isEmpty()) {
            return null;
        }

        Set<Map.Entry<String, List<JsonObject>>> entrySet = shardResults.entrySet();
        List<JsonObject> results = null;
        JsonObject doc = null;
        String shard = null;
        long start = System.currentTimeMillis();
        ShardSortedQueue queue = new ShardSortedQueue(fields, size);
        for (Map.Entry<String, List<JsonObject>> entry : entrySet) {
            shard = entry.getKey();
            results = entry.getValue();
            for (int i = 0; i < results.size(); i++) {
                doc = results.get(i);
                ShardRsp shardRsp = new ShardRsp(fields, results);
                shardRsp.id = doc.get(uniqueKey).getAsString();
                shardRsp.setShard(shard);
                shardRsp.orderInShard = i;
                queue.insertWithOverflow(shardRsp);
            }
        }
        String[] returnIds = new String[size];
        for (int i = size - 1; i >= 0; i--) {
            ShardRsp shardRsp = queue.pop();
            returnIds[i] = shardRsp.getSortFieldValues().get(shardRsp.orderInShard).get(uniqueKey).getAsString();
        }

        long timeCost = (System.currentTimeMillis() - start);
        logger.info("sort cost {} secs", timeCost);

        return returnIds;
    }

}
