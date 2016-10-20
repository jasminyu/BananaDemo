package com.etisalat.log.sort;

public class SortField {
    private Type type;
    private String name;
    private boolean reverse;
    public SortField(String name, boolean reverse, Type type) {
        this.name = name;
        this.reverse = reverse;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    public static enum Type {
        string,
        long_n,
        integer,
        double_n,
        float_n,
        tdate,
        text_general
    }
}
