package edu.hust.soict.bigdata.facilities.model;

import java.io.Serializable;

public interface DataModel extends Serializable {

    void setId(String id);

    String getId();

    @Override
    String toString();
}
