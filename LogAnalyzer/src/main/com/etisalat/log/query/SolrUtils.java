package com.etisalat.log.query;

import com.etisalat.log.common.LogQueryException;
import com.etisalat.log.config.LogConfFactory;
import com.etisalat.log.config.LogConstants;
import com.etisalat.log.sort.Schema;
import com.etisalat.log.sort.SortField;
import com.google.gson.*;
import com.huawei.solr.client.solrj.impl.InsecureHttpClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SolrUtils {
    private static final Logger logger = LoggerFactory.getLogger(SpnegoService.class);
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final Random random = new Random(formatSeed(new Random().nextLong()).hashCode());

    private static AtomicLong cacheKey = new AtomicLong(System.currentTimeMillis());

    public static CloudSolrClient getSolrClient() throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, LogConfFactory.maxConnections);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, LogConfFactory.maxConnectionsPerHost);

        HttpClient httpClient = HttpClientUtil.createClient(params);
        httpClient = new InsecureHttpClient(httpClient, params);

        LBHttpSolrClient lbHttpSolrClient = new LBHttpSolrClient(httpClient);
        lbHttpSolrClient.setRequestWriter(new BinaryRequestWriter());

        CloudSolrClient solrClient = new CloudSolrClient(LogConfFactory.zkHost, lbHttpSolrClient);
        solrClient.setZkClientTimeout(LogConfFactory.zkClientTimeout);
        solrClient.setZkConnectTimeout(LogConfFactory.zkConnectTimeout);

        solrClient.connect();

        logger.info("The cloud Server has been connected.");

        return solrClient;
    }

    protected static String generateCacheKey() {
        String timestamp = String.valueOf(System.nanoTime());
        return timestamp + LogConstants.CACHE_KEY_SEPARATOR + Hash
                .murmurhash3_x86_32(timestamp, 0, timestamp.length(), 0);
    }

    protected static String generateCacheKey2() {
        return String.valueOf(cacheKey.addAndGet(1l));
    }

    private static <T> T deepCopy(T object, Class<T> type) {
        try {
            Gson gson = new Gson();
            return gson.fromJson(gson.toJson(object, type), type);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected static JsonObject deepCopyJsonObj(JsonObject oldJsonObject) {
        return deepCopy(oldJsonObject, JsonObject.class);
    }

    public static void handSelectReqException(String msg, int msgCode, ServletResponse response) {
        if (response == null) {
            return;
        }

        JsonObject rspJson = getErrJsonObj(msg, msgCode);

        PrintWriter out;
        try {
            out = response.getWriter();
            out.print(rspJson.toString());
            response.setContentType("text/plain; charset=UTF-8");
            response.getWriter().flush();
            out.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return;
    }
    
    public static JsonObject getErrJsonObj(String msg, int msgCode) {
    	
    	JsonObject rspJson = new JsonObject();
    	JsonObject errorJson =new JsonObject();
    	JsonObject responseJson =new JsonObject();
    	
    	responseJson.addProperty("numFound", 0);
    	rspJson.add("response", responseJson);
    	
    	errorJson.addProperty("msg", msg);
    	errorJson.addProperty("code", msgCode);
    	rspJson.add("error", errorJson);
    	
    	return rspJson;
    }

    private static String formatSeed(long seed) {
        StringBuilder b = new StringBuilder();

        do {
            b.append(HEX[(int) (seed & 15L)]);
            seed >>>= 4;
        } while (seed != 0L);

        return b.reverse().toString();
    }

    protected static void addJsonElement(JsonObject jsonObject, String col, byte[] bytes) {
        if (bytes == null) {
            return;
        }

        SortField.Type type = LogConfFactory.columnQualifiersTypeMap.get(col);
        if (type == null) {
            jsonObject.addProperty(col, Bytes.toString(bytes));
            return;
        }
        addJsonElement(jsonObject, col, bytes, type);
    }

    private static void addJsonElement(JsonObject jsonObject, String col, byte[] bytes, SortField.Type type) {
        switch (type) {
        case double_n:
            jsonObject.addProperty(col, new Double(Bytes.toDouble(bytes)));
            break;
        case float_n:
            jsonObject.addProperty(col, new Float(Bytes.toFloat(bytes)));
            break;
        case integer:
            jsonObject.addProperty(col, new Integer(Bytes.toInt(bytes)));
            break;
        case long_n:
            jsonObject.addProperty(col, new Long(Bytes.toLong(bytes)));
            break;
        case tdate:
        case string:
        case text_general:
            jsonObject.addProperty(col, Bytes.toString(bytes));
            break;
        default:
            throw new RuntimeException("The type " + type + " does not support");
        }
    }

    public static Schema schemaFileToBean() throws IOException {
        try (InputStream in = new FileInputStream(new File(LogConfFactory.schemaPath))) {
            return schemaToBean(in);
        } catch (IOException e) {
            throw e;
        }
    }

    private static Schema schemaToBean(InputStream in) throws IOException {
        try {
            JAXBContext jc = JAXBContext.newInstance(Schema.class);
            Unmarshaller un = jc.createUnmarshaller();
            return (Schema) un.unmarshal(in);
        } catch (JAXBException e) {
            throw fixExceptionToIOE(e, "Failed to parse schema.xml data to bean.");
        }
    }

    private static IOException fixExceptionToIOE(Throwable e, String... msg) {
        if (null == e) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (String message : msg) {
            builder.append(message).append(",");
        }
        IOException ne = new IOException(0 == builder.length() ? e.getMessage() : builder.toString());
        ne.setStackTrace(e.getStackTrace());
        ne.initCause(e.getCause());
        return ne;
    }

    protected static String getSolrKey(byte[] rowKey) {
        StringBuilder builder = new StringBuilder();
        byte[] timestamp = new byte[8];
        byte[] taskId = new byte[2];
        for (int i = 0; i < 8; i++) {
            timestamp[7 - i] = rowKey[i];
        }
        taskId[0] = rowKey[8];
        taskId[1] = rowKey[9];
        builder.append(Bytes.toLong(timestamp));
        builder.append("#");
        builder.append(Bytes.toShort(taskId));
        return builder.toString();
    }

    protected static byte[] exchangeKey(String solrKey) {
        String[] values = solrKey.split("#");
        byte[] timeBytes = Bytes.toBytes(Long.valueOf(values[0]).longValue());
        int timeLen = timeBytes.length;
        for (int i = 0; i < timeLen / 2; ++i) {
            byte tmp = timeBytes[i];
            timeBytes[i] = timeBytes[timeLen - i - 1];
            timeBytes[timeLen - i - 1] = tmp;
        }
        return Bytes.add(timeBytes, Bytes.toBytes(Short.valueOf(values[1]).shortValue()));
    }

    public static String getSortStr(List<String> sortStrList, String rowkeySortStr) {
        StringBuilder builder = new StringBuilder("&sort=");
        if (rowkeySortStr != null) {
            builder.append(rowkeySortStr);
        }

        for (String sortStr : sortStrList) {
            builder.append(",").append(sortStr);
        }

        return builder.toString();
    }

    public static String getSortStr(List<String> sortStrList) {
        if (sortStrList.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder("&sort=");
        boolean isFirst = true;
        for (String sortStr : sortStrList) {
            if (!isFirst) {
                builder.append(",");
            }
            builder.append(sortStr);
        }

        return builder.toString();
    }

    protected static String getRandomReplicaBaseUrl(Slice slice) {
        Collection<Replica> replicas = slice.getReplicas();
        List<String> randomizedReplicasUrls = new ArrayList<String>();
        for (Replica replica : replicas) {
            if ("active".equals(replica.getStr(ZkStateReader.STATE_PROP))) {
                randomizedReplicasUrls.add(replica.getStr(ZkStateReader.BASE_URL_PROP) + "/" + replica
                        .getStr(ZkStateReader.CORE_NAME_PROP) + "/");
            }
        }
        Collections.shuffle(randomizedReplicasUrls, random);
        if (randomizedReplicasUrls.isEmpty()) {
            return null;
        } else {
            return randomizedReplicasUrls.get(0);
        }
    }

    protected static long getNumFound(SolrQueryRsp solrQueryRsp) {
        if (solrQueryRsp == null || solrQueryRsp.getQueryResponse() == null) {
            return 0l;
        }

        return solrQueryRsp.getQueryResponse().getResults().getNumFound();
    }

    protected static String getNextCollection(String collection, String maxCollection) throws LogQueryException {
        if (collection.compareTo(maxCollection) >= 0) {
            return collection;
        }
        String dateStr = collection.substring(LogConfFactory.collectionPrefixLen);
        String newCollection = LogConfFactory.collectionPrefix + getNextDateString(dateStr);
        if (newCollection.compareTo(maxCollection) >= 0) {
            return maxCollection;
        } else {
            return newCollection;
        }
    }

    private static String getNextDateString(String dateStr) throws LogQueryException {
        Calendar rightNow = Calendar.getInstance();
        Date date = null;
        try {
            date = LogConfFactory.collectionSuffixDateFormat.parse(dateStr);
        } catch (ParseException e) {
            String errMsg = "failed to parse date string: " + dateStr + " with date format: "
                    + LogConfFactory.collectionSuffixDateFormatStr;
            logger.error(errMsg);
            throw new LogQueryException(errMsg, e);
        }

        rightNow.setTime(date);
        rightNow.add(Calendar.DAY_OF_YEAR, +1);
        return LogConfFactory.collectionSuffixDateFormat.format(rightNow.getTime());
    }

    protected static String getNextShardId(String shardId) throws LogQueryException {
        return "_shard" + (getIdOfShardId(shardId) + 1);
    }

    public static Integer compareShardId(String shard, String secondShard) throws LogQueryException {
        return getIdOfShardId(shard).compareTo(getIdOfShardId(secondShard));
    }

    public static Integer getIdOfShardId(String shard) throws LogQueryException {
        try{
            return Integer.valueOf(shard.substring("_shard".length()));
        }catch(RuntimeException e) {
            String errMsg = "shardId is invalid, it should be like shard+number, such as shard1, But it is " + shard;
            logger.error(errMsg);
            throw new LogQueryException(errMsg);
        }
    }

    protected static String getCollWithShardId(String collection, String shardId) {
        return collection + shardId;
    }

    protected static String getCsvString(JsonArray jsonArray) {
        if (jsonArray.size() == 0) {
            return "";
        }

        String csvHeader = null;
        StringBuilder builder = new StringBuilder();

        String value = null;
        for (JsonElement jsonObj : jsonArray) {
            if (LogConfFactory.keepCsvHeader && csvHeader == null) {
                csvHeader = getCsvHeader(jsonObj);
                builder.append(csvHeader).append("\n");
            }
            boolean first = true;
            Set<Map.Entry<String, JsonElement>> set = jsonObj.getAsJsonObject().entrySet();
            for (Map.Entry<String, JsonElement> entry : set) {
                value = entry.getValue() instanceof JsonNull ? "" : entry.getValue().getAsString();
                if (first) {
                    builder.append(value);
                    first = false;
                }
                builder.append(",").append(value);
            }

            builder.append("\n");
        }

        return builder.toString();
    }

    protected static String getCsvHeader(JsonElement jsonObject) {
        Set<Map.Entry<String, JsonElement>> set = jsonObject.getAsJsonObject().entrySet();
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, JsonElement> entry : set) {
            builder.append(",").append(entry.getKey());
        }

        return builder.delete(0, 1).toString();
    }

    protected static String getCollection(String collectionWithShardId) {
        return collectionWithShardId.substring(0, LogConfFactory.collectionNameLen);
    }

    protected static String getShardId(String collectionWithShardId) {
        return collectionWithShardId.substring(LogConfFactory.collectionNameLen);
    }

    protected static long getSingleFileRecordNum(long realReturnNum) {
        return realReturnNum <= LogConfFactory.downloadedSingleFileSize ?
                realReturnNum :
                LogConfFactory.downloadedSingleFileSize;
    }
}
