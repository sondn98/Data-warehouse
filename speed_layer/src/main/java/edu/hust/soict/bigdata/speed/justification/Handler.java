package edu.hust.soict.bigdata.speed.justification;

import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Handler<T> {
    Properties conf = new Properties();
    Logger logger = LoggerFactory.getLogger(Handler.class);

    T handle(T object);
}
