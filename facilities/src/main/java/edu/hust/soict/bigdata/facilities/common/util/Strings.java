package edu.hust.soict.bigdata.facilities.common.util;

import java.util.Locale;

/**
 * @author sondn
 * @since 1/4/2020
 **/

public class Strings {

    public static String format(String format, Object... params) {
        return String.format(Locale.US, format, params);
    }

    public static boolean isNullOrEmpty(String content) {
        return (content == null) || (content.isEmpty());
    }

    public static boolean isNotNullOrEmpty(String content){
        return !isNullOrEmpty(content) && !(content.toLowerCase().equals("null") || content.toLowerCase().equals("(null)"));
    }
}
