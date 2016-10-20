package com.etisalat.log.sort;

import com.google.gson.JsonObject;

import java.util.Comparator;
import java.util.List;

public class JSONObjComparator implements Comparator<JsonObject> {

    private List<SortField> sortFieldList;

    public JSONObjComparator(List<SortField> sortFieldList) {
        this.sortFieldList = sortFieldList;
    }

    public static void main(String[] args) {
        JsonObject jsonObject1 = new JsonObject();
        jsonObject1.addProperty("row", System.nanoTime());
        JsonObject jsonObject2 = new JsonObject();
        jsonObject2.addProperty("row", System.nanoTime());

        System.out.println(jsonObject1.equals(jsonObject2));
    }

    @Override
    public int compare(JsonObject o1, JsonObject o2) {
        int cmp = 0;
        for (SortField sortField : sortFieldList) {
            cmp = SortUtils.compare(sortField, o1.get(sortField.getName()), o2.get(sortField.getName()));
            //            cmp = sortField.isReverse() ? -cmp : cmp;
            if (cmp != 0) {
                return cmp;
            }
        }

        return cmp;
    }
}
