package edu.hust.soict.bigdata.facilities.model;

import java.io.Serializable;

public abstract class DataModel implements Serializable {

    public String id;
    public Long eventTime;

    public DataModel(){
        this.eventTime = System.currentTimeMillis();
    }

//    public void setId(String id){
//        this.id = id;
//    }
//
//    public String getId(){
//        return this.id;
//    }
//
//    public Long getEventTime(){
//        return this.eventTime;
//    }

    @Override
    public abstract String toString();
}
