package edu.hust.soict.bigdata.collector.worker;

import edu.hust.soict.bigdata.collector.action.ActionChecksum;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;

public class Checksumer {
    private static final Logger logger = LoggerFactory.getLogger(Checksumer.class);

    public static void main(String[] args) throws IOException {
        Properties props = new Properties().toSubProperties("data-scraping");

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(
                new ActionChecksum("ecommerce_review", props), 1000, 1000);
    }
}
