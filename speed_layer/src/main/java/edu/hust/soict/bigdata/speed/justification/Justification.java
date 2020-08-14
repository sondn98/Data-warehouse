package edu.hust.soict.bigdata.speed.justification;

import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public interface Justification<T> {

    /*
    * Handlers must have same type parameter
    * */
    List<Handler<?>> handlers = new LinkedList<>();

    @SuppressWarnings("unchecked")
    default List<T> justify(List<T> input, List<Handler<?>> handlers){
        List<T> output = new LinkedList<>();
        for(T object : input){
            if(null == object) continue;
            for(Handler<?> handler : handlers){
                object = ((Handler<T>)handler).handle(object);
            }
            output.add(object);
        }

        return output;
    }

    default Justification<T> setHandler(Collection<String> clazzes){
        for(String clazz : clazzes) {
            Handler<T> handler = Reflects.newInstance(clazz);
            handlers.add(handler);
        }
        return this;
    }

    default List<T> handle(List<T> input){
        if(handlers.isEmpty()) setHandler(Config.getCollection("messages.justification.handler.classes"));
        List<T> output = justify(input, handlers);
        save(output);
        return output;
    }

    default void save(List<T> product){

    }
}
