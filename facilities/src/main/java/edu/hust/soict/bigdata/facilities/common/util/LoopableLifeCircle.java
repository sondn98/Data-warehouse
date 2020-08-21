package edu.hust.soict.bigdata.facilities.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LoopableLifeCircle implements Runnable{

    private AtomicBoolean running;

    private static final Logger logger = LoggerFactory.getLogger(LoopableLifeCircle.class);

    public LoopableLifeCircle(){
        this.running = new AtomicBoolean(false);
    }

    public void start(){
        if(running.get()){
            logger.info("Job is running and can not be duplicated");
            return;
        }

        logger.info("Start a loop-able life circle thread");
        running.compareAndSet(false, true);
        while(running.get()){
            run();
        }
        logger.info("A loop-able life circle thread stopped");
    }

    public void stop(){
        logger.info("Stop a loop-able life circle thread");
        this.running.compareAndSet(true, false);
    }

    public void restart(){
        this.stop();
        this.start();
    }

    public boolean isRunning(){
        return running.get();
    }

}
