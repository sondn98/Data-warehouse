package edu.hust.soict.bigdata.facilities.platform.zookeeper;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZKClient implements AutoCloseable{

    private ZooKeeper zookeeper;
    private static final Logger logger = LoggerFactory.getLogger(ZKClient.class);

    public ZKClient(String name) {
        this.zookeeper = ZookeeperClientProvider
                .getInstance()
                .getOrCreate(name);
    }

    public boolean reconnect(){
        this.zookeeper = ZookeeperClientProvider
                .getInstance()
                .getOrCreate(Config.getProperty(Const.ZK_CLIENT_CONNECTION_NAME, "default"));

        return zookeeper != null;
    }

    public void create(String path, String data) throws KeeperException, InterruptedException {
        zookeeper.create(
                path,
                data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zookeeper.exists(path, null) != null;
    }

    public void createEphemeral(String path, String data) throws KeeperException, InterruptedException {
        zookeeper.create(
                path,
                data.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
    }

    public String getData(String path, boolean delete) throws KeeperException, InterruptedException {
        String data = new String(zookeeper.getData(path, null, null), StandardCharsets.UTF_8);
        if(delete)
            zookeeper.delete(path, zookeeper.exists(path, true).getVersion());

        return data;
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zookeeper.setData(path, data.getBytes(), -1);
    }

    public List<String> listChilds(String parentPath) throws KeeperException, InterruptedException {
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
