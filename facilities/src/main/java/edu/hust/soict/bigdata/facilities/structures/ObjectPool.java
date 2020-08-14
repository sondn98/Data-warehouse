package edu.hust.soict.bigdata.facilities.structures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class ObjectPool<T> {

    protected static final Logger logger = LoggerFactory.getLogger(ObjectPool.class);

    private Map<String, T> poolObjects;

    public ObjectPool(){
        this.poolObjects = new HashMap<>();
    }

    public synchronized T getOrCreate(String name){
        if(poolObjects.containsKey(name))
            return poolObjects.get(name);

        logger.info("There is no key " + name + " exist. Tend to create a new object");
        T obj = this.create();
        poolObjects.put(name, obj);
        return obj;
    }

    protected abstract T create();
}
