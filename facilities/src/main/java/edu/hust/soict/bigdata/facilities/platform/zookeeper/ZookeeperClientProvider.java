package edu.hust.soict.bigdata.facilities.platform.zookeeper;

import edu.hust.soict.bigdata.facilities.structures.ObjectPool;

import java.io.IOException;

public class ZookeeperClientProvider extends ObjectPool<ZKClient> {

    @Override
    protected ZKClient create() {
        try {
            return new ZKClient();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
