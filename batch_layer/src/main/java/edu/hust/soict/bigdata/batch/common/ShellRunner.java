package edu.hust.soict.bigdata.batch.common;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ShellRunner {

    private static final Logger logger = LoggerFactory.getLogger(ShellRunner.class);
    public static int run(String script){
        CommandLine cmd = new CommandLine(script);
        Executor executor = new DefaultExecutor();

        try {
            return executor.execute(cmd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
