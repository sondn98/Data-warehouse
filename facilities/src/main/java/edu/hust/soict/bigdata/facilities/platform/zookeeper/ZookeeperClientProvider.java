package edu.hust.soict.bigdata.facilities.platform.zookeeper;

import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;

import java.io.IOException;

public class ZookeeperClientProvider extends ObjectPool<ZKClient> {

    @Override
    protected ZKClient create(Properties props) {
        try {
            return new ZKClient(props);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
