package edu.hust.soict.bigdata.facilities.platform.hive;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.apache.hive.jdbc.HiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class HiveConnectionProvider {

    private static final Map<String, Connection> connectionMap = new HashMap<>();
    private static final HiveDriver driver = new HiveDriver();
    public static final Logger logger = LoggerFactory.getLogger(HiveConnectionProvider.class);

    public static Connection getOrCreate(String connName, Properties props) throws SQLException {
        if(connectionMap.containsKey(connName)){
            return connectionMap.get(connName);
        } else{
            String connectionStr = props.getProperty(Const.HIVE_CONNECTION_URL, "jdbc:hive2://localhost:10000/default");
            Connection conn = driver.connect(connectionStr, props);
            connectionMap.put(connName, conn);
            logger.info("Created connection. Connection string: " + connectionStr);
            return conn;
        }
    }

    public static void main(String[] args) throws SQLException {
        Connection conn = HiveConnectionProvider.getOrCreate("test_connection", new Properties());
        boolean check = conn.isValid(10);
        if (check){
            logger.info("Connection is okay");
        } else{
            logger.warn("Connection fail");
        }
    }
}
