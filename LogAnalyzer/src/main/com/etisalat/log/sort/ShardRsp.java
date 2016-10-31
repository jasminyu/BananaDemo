package com.etisalat.log.sort;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ShardRsp {
    public String shard;
    public int orderInShard;
    public String id;
    public List<SortField> fields;
    List<JsonObject> sortFieldValues;

    public ShardRsp(List<SortField> fields, List<JsonObject> sortFieldValues) {
        this.fields = fields;
        this.sortFieldValues = sortFieldValues;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getOrderInShard() {
        return orderInShard;
    }

    public void setOrderInShard(int orderInShard) {
        this.orderInShard = orderInShard;
    }

    public String getShard() {
        return shard;
    }

    public void setShard(String shard) {
        this.shard = shard;
    }

    public List<JsonObject> getSortFieldValues() {
        return sortFieldValues;
    }

    public void setSortFieldValues(List<JsonObject> sortFieldValues) {
        this.sortFieldValues = sortFieldValues;
    }

    public List<SortField> getFields() {
        return fields;
    }

    public void setFields(List<SortField> fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ShardRsp shardRsp = (ShardRsp) o;

        if (id != null ? !id.equals(shardRsp.id) : shardRsp.id != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}

class ShardSortedQueue extends PriorityQueue<ShardRsp> {
    protected List<SortField> fields;
    protected Comparator<ShardRsp>[] comparators;

    public ShardSortedQueue(List<SortField> sortFields, int size) {
        super(size);
        final int n = sortFields.size();
        //noinspection unchecked
        comparators = new Comparator[n];
        this.fields = new ArrayList<SortField>(n);
        for (int i = 0; i < n; ++i) {
            comparators[i] = comparatorFieldComparator(sortFields.get(i));
            fields.add(sortFields.get(i));
        }

    }

    @Override
    protected boolean lessThan(ShardRsp docA, ShardRsp docB) {
        if (docA.shard == docB.shard) {
            return !(docA.orderInShard < docB.orderInShard);
        }

        // run comparators
        final int n = comparators.length;
        int c = 0;
        for (int i = 0; i < n && c == 0; i++) {
            c = comparators[i].compare(docA, docB);
        }

        if (c == 0) {
            c = -docA.shard.compareTo(docB.shard);
        }
        return c < 0;
    }

    Comparator<ShardRsp> comparatorFieldComparator(SortField sortField) {
        return new ShardComparator(sortField) {
            // Since the PriorityQueue keeps the biggest elements by default,
            // we need to reverse the field compare ordering so that the
            // smallest elements are kept instead of the largest... hence
            // the negative sign.
            @Override
            public int compare(final ShardRsp o1, final ShardRsp o2) {
                //noinspection unchecked
                String a = sortVal(o1).toString();
                String b = sortVal(o2).toString();
                try {
                    return -SortUtils.compare(sortField, sortVal(o1), sortVal(o2));
                } catch (Exception t) {
                    t.printStackTrace();
                    System.out.println("ERROR a:" + a + ", b:" + b);
                    throw t;
                }
            }
        };
    }

    abstract class ShardComparator implements Comparator<ShardRsp> {
        final SortField sortField;
        final String fieldName;

        public ShardComparator(SortField sortField) {
            this.sortField = sortField;
            this.fieldName = sortField.getName();
        }

        JsonElement sortVal(ShardRsp shardDoc) {
            return shardDoc.sortFieldValues.get(shardDoc.orderInShard).get(fieldName);
        }
    }

}
