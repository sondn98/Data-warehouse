package edu.hust.soict.bigdata.facilities.model;

import java.io.Serializable;

public abstract class DataModel implements Serializable {

    private String id;

    public void setId(String id){
        this.id = id;
    }

    public String getId(){
        return this.id;
    }

    @Override
    public abstract String toString();
}
