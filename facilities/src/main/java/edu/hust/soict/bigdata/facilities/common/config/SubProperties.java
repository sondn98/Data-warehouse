package edu.hust.soict.bigdata.facilities.common.config;

import java.util.Map;

public class SubProperties extends Properties {
    private String prefix;

    SubProperties(String prefix, Properties p) {
        for(Map.Entry<Object, Object> kv : p.entrySet()){
            this.put(kv.getKey(), kv.getValue());
        }

        if(prefix == null)
            throw new RuntimeException("Key prefix in sub-properties can not be null");
        this.prefix = prefix;
    }

    @Override
    public String getProperty(String key){
        if(this.containsKey(keyWithPrefix(key))){
            return super.getProperty(keyWithPrefix(key));
        } else{
            return super.getProperty(key);
        }
    }

    private String keyWithPrefix(String key) {
        return prefix + "." + key;
    }
}
