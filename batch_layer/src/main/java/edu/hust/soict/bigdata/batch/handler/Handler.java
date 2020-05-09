package edu.hust.soict.bigdata.batch.handler;

import edu.hust.soict.bigdata.facilities.model.DataModel;

public interface Handler<M extends DataModel> {

    void handle();

}
