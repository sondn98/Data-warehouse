package edu.hust.soict.bigdata.facilities.platform.zookeeper;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperClientProvider extends ObjectPool<ZooKeeper> {

    private static ZookeeperClientProvider zProvider;

    public static ZookeeperClientProvider getInstance(){
        if(zProvider == null)
            zProvider = new ZookeeperClientProvider();
        return zProvider;
    }

    @Override
    protected ZooKeeper create() {
        try {

            String host = Config.getProperty(Const.ZK_HOST, "localhost");
            int sessionTimeout = Config.getIntProperty(Const.ZK_CLIENT_SESSION_TIMEOUT, 2000);
            logger.info("Creating zookeeper client on zk server: " + host);
            return new ZooKeeper(host, sessionTimeout, watchedEvent -> {
                // ignored
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
