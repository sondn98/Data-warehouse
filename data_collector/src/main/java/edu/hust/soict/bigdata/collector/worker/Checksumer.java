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
        Timer timer = new Timer();
        logger.info("Starting checksumer");
        timer.scheduleAtFixedRate(
                new ActionChecksum("ecommerce_review"), 1000, 1000);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Checksumer stopped")));
    }
}
