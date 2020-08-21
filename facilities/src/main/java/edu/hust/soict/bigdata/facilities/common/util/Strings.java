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

    public static String concatFilePath(String parent, String child){
        if(parent.endsWith("/"))
            parent = parent.substring(0, parent.length() - 1);
        if(child.startsWith("/"))
            return parent + child;
        else return parent + "/" + child;
    }

    public static String concatFilePath(String... parts){
        StringBuilder path = new StringBuilder();
        path.append(parts[0].endsWith("/") ? parts[0].substring(0, parts[0].length() - 1) : parts[0]);
        for(int i = 1 ; i < parts.length ; i ++){
            if(parts[i].endsWith("/"))
                parts[i] = parts[i].substring(0, parts[i].length() - 1);
            if(parts[i].startsWith("/"))
                path.append(parts[i]);
            else path.append("/").append(parts[i]);
        }

        return path.toString();
    }

    public static boolean isNullOrEmpty(String content) {
        return (content == null) || (content.isEmpty());
    }

    public static boolean isNotNullOrEmpty(String content){
        return !isNullOrEmpty(content) &&
                !(content.toLowerCase().equals("null") || content.toLowerCase().equals("(null)"));
    }
}
