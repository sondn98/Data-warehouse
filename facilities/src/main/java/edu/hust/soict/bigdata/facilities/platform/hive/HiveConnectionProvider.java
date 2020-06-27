package edu.hust.soict.bigdata.facilities.platform.hive;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.SQLException;

public class HiveConnectionProvider extends ObjectPool<Connection> {

    private static final HiveDriver driver = new HiveDriver();

    @Override
    protected Connection create() {
        String connectionStr = Properties
                .getProperty(Const.HIVE_CONNECTION_URL, "jdbc:hive2://localhost:10000/default");
        try {
            return driver.connect(connectionStr, Properties.getProps());
        } catch (SQLException e) {
            throw new RuntimeException("Can not create connection to hive server", e);
        }
    }
}
