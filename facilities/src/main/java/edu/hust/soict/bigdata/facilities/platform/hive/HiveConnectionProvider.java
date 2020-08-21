package edu.hust.soict.bigdata.facilities.platform.hive;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;

public class HiveConnectionProvider extends ObjectPool<Connection> {

    private static HiveConnectionProvider hiveProvider;

    public static HiveConnectionProvider getInstance(){
        if(hiveProvider == null)
            hiveProvider = new HiveConnectionProvider();
        return hiveProvider;
    }

    @Override
    protected Connection create() {
        logger.info("Creating jdbc connection to Hive");
        String connectionStr = Config
                .getProperty(Const.HIVE_CONNECTION_URL, "jdbc:hive2://localhost:10000/default");
        try {
            return new HiveDriver().connect(connectionStr, Config.getProps());
        } catch (SQLException e) {
            logger.error("Error while creating jdbc connection to Hive", e);
            return null;
        }
    }
}
