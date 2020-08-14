package edu.hust.soict.bigdata.speed.justification;

import edu.hust.soict.bigdata.facilities.common.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Handler<T> {
    Config conf = new Config();
    Logger logger = LoggerFactory.getLogger(Handler.class);

    T handle(T object);
}
