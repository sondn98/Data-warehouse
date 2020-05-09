package edu.hust.soict.bigdata.facilities.platform.zookeeper;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZKClient implements AutoCloseable{

    private ZooKeeper zookeeper;
    private static final Logger logger = LoggerFactory.getLogger(ZKClient.class);

    public ZKClient(Properties props) throws IOException {
        String host = props.getProperty(Const.ZK_HOST, "localhost");
        int sessionTimeout = props.getIntProperty(Const.ZK_CLIENT_SESSION_TIMEOUT, 2000);
        zookeeper = new ZooKeeper(host, sessionTimeout, watchedEvent -> {
            // ignored
        });

        logger.info("Created zookeeper client on zk server: " + host);
    }

    public void create(String path, String data) throws KeeperException, InterruptedException {
        zookeeper.create(
                path,
                data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    public synchronized String getDataAndDelete(String path) throws KeeperException, InterruptedException {
        String data = new String(zookeeper.getData(path, null, null), StandardCharsets.UTF_8);
        zookeeper.delete(path, zookeeper.exists(path, true).getVersion());

        return data;
    }

    public synchronized List<String> listChilds(String parentPath) throws KeeperException, InterruptedException {
        return zookeeper.getChildren(parentPath, null);
    }

    public String oldestChild(List<String> childs) throws KeeperException, InterruptedException {
        long minCTime = Long.MAX_VALUE;
        String selected = null;
        for(String child : childs){
            long cTime = zookeeper.exists(child, null).getCtime();
            selected = cTime < minCTime ? child : selected;
        }

        return selected;
    }

    @Override
    public void close() throws InterruptedException {
        zookeeper.close();
    }
}
