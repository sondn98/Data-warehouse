package edu.hust.soict.bigdata.facilities.structures;

import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class ObjectPool<T> {

    private static Map<String, Object> poolObjects = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ObjectPool.class);

    public static synchronized <T> T getOrCreate(String name, Class<T> clazz){
        if(poolObjects.containsKey(name))
            return clazz.cast(poolObjects.get(name));

        logger.info("There is no key " + name + " exist. Tend to create a new object");
        T obj = Reflects.newInstance(clazz, new Class[]{});
        poolObjects.put(name, obj);
        return obj;
    }

    protected abstract T create();
}
