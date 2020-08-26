package edu.hust.soict.bigdata.facilities.structures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ObjectPool<T extends AutoCloseable> {

    protected static final Logger logger = LoggerFactory.getLogger(ObjectPool.class);

    private Map<String, T> poolObjects;

    public ObjectPool(){
        this.poolObjects = new ConcurrentHashMap<>();
    }

    public synchronized T getOrCreate(String name){
        if(poolObjects.containsKey(name))
            return poolObjects.get(name);

        logger.info("There is no key " + name + " exist. Tend to create a new object");
        T obj = this.create();
        if(obj != null){
            poolObjects.put(name, obj);
            return obj;
        }

        return null;
    }

    public synchronized T recreate(String name) throws Exception {
        this.remove(name);
        return getOrCreate(name);
    }

    public synchronized void put(String name, T obj){
        poolObjects.putIfAbsent(name, obj);
    }

    public boolean has(String name){
        return poolObjects.containsKey(name);
    }

    public synchronized void remove(String name) throws Exception {
        if(!has(name)) {
            logger.info("No object named " + name + " found. Can not be removed");
            return;
        }
        getOrCreate(name).close();
        poolObjects.remove(name);
    }

    public int size(){
        return poolObjects.size();
    }

    protected abstract T create();
}
