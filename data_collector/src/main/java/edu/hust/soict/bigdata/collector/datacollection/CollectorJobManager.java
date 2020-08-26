package edu.hust.soict.bigdata.collector.datacollection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.zookeeper.ZKClient;
import edu.hust.soict.bigdata.facilities.structures.ObjectPool;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CollectorJobManager extends ObjectPool<CollectorJob> implements AutoCloseable{

    private static ExecutorService service;
    private static CollectorJobManager manager;

    private CollectorJobManager() throws KeeperException, InterruptedException, IOException {
        if(service == null || service.isTerminated())
            service = Executors.newFixedThreadPool(Config.getIntProperty(CollectorConst.COLLECTOR_JOB_POOL_SIZE, 10));

        ObjectMapper om = new ObjectMapper();
        ZKClient client = new ZKClient(Config.getProperty(Const.ZK_CLIENT_CONNECTION_NAME));
        String parent = Strings.concatFilePath(Config.getProperty(CollectorConst.COLLECTION_JOB_ZK_PARENT), CollectorConst.HOST_NAME);
        if(!client.exists(parent))
            client.create(parent, "");
        else{
            List<String> childs = client.listChilds(parent);
            boolean startup = Config.getBoolProperty(CollectorConst.COLLECTOR_JOB_START_UP_ON_MANAGER_INIT, false);
            for(String child : childs){
                String node = Strings.concatFilePath(parent, child);
                String data = client.getData(node, false);
                CollectorJobAttributes attributes = om.readValue(data, CollectorJobAttributes.class);
                Config.setRuntimeObj(CollectorConst.COLLECTOR_JOB_ATTR_OBJECT_KEY, attributes);
                CollectorJob job = getOrCreate(attributes.SCHEMA_NAME);
                if(startup)
                    job.start();
            }
        }
    }

    public static CollectorJobManager getInstance() throws KeeperException, InterruptedException, IOException {
        if(manager == null)
            manager = new CollectorJobManager();

        return manager;
    }

    public <M extends DataModel> void collect(M data){
        String schemaName = data.getClass().getName();
        if(!super.has(schemaName)){
            logger.warn("No schema named " + schemaName + " found");
            return;
        }

        CollectorJob job = super.getOrCreate(schemaName);
        if(!job.isRunning()){
            logger.info("Collector job for schema " + schemaName + " was stopped. Start it first then it can perform collection");
            return;
        }

        job.collect(data);
    }

    public synchronized void stop(String name, boolean delete) throws Exception {
        if(super.has(name)){
            super.getOrCreate(name).stop();
            if(delete)
                super.remove(name);
        }
    }

    public synchronized void start(String name, CollectorJobAttributes attributes){
        if(attributes != null)
            Config.setRuntimeObj(CollectorConst.COLLECTOR_JOB_ATTR_OBJECT_KEY, attributes);

        service.submit(() -> super.getOrCreate(name).start());
    }

    public synchronized void restart(String name, CollectorJobAttributes attributes) throws Exception {
        if(super.has(name)){
            this.stop(name, false);
            if(attributes != null)
                Config.setRuntimeObj(CollectorConst.COLLECTOR_JOB_ATTR_OBJECT_KEY, attributes);
            super.remove(name);
            this.start(name, attributes);
        }
    }

    @Override
    public synchronized void remove(String name) throws Exception {
        if(super.has(name)){
            if(getOrCreate(name).isRunning()){
                logger.info("Job " + name + " is running. Stop it first before remove");
            }

            super.remove(name);
        }
    }

    public boolean isRunning(String name){
        if(super.has(name))
            return getOrCreate(name).isRunning();

        return false;
    }

    @Override
    protected CollectorJob create() {
        try {
            return new CollectorJob();
        } catch (KeeperException | InterruptedException | JsonProcessingException e) {
            logger.info("Fail to create collector job", e);
            return null;
        }
    }

    @Override
    public void close() throws Exception {

    }
}
