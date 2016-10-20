package com.etisalat.log.common;

import org.apache.commons.lang.StringUtils;

public class AssertUtil {

    public static void notNull(Object obj, String msg, Object... args) {
        if (obj == null) {
            throw new NullPointerException(String.format(msg, args));
        }
    }

    public static void notBlank(String str, String msg, Object... args) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(String.format(msg, args));
        }
    }

    public static void isTrue(boolean value, String msg) {
        if (!value) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void noneNull(String msg, Object... valuesToTest) {
        for (int i = 0; i < valuesToTest.length; i++) {
            Object obj = valuesToTest[i];
            notNull(obj, msg + " index " + i);
        }
    }

    public static void nonBlank(String msg, String... valuesToTest) {
        for (int i = 0; i < valuesToTest.length; i++) {
            String obj = valuesToTest[i];
            notBlank(obj, msg + " index " + i);
        }
    }

    public static void larger(String msg, long left, long right) {
        if (left <= right) {
            throw new IllegalArgumentException(msg);
        }
    }

}