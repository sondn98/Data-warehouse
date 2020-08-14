package edu.hust.soict.bigdata.facilities.common.wal;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.wal.impl.LocalWalFile;
import edu.hust.soict.bigdata.facilities.common.wal.impl.csv.CSVWalReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.csv.CSVWalWriter;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONWriter;
import edu.hust.soict.bigdata.facilities.common.wal.impl.object.ObjectReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.object.ObjectWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class WalFactory {

    private static final Logger logger = LoggerFactory.getLogger(WalFactory.class);

    public static <T extends DataModel> WalReader<T> getReader(WalFile file) throws IOException {
        String codec = file.codec();
        switch(codec){
            case "object":
                return new ObjectReader<>(file);
            case "json":
                return new JSONReader<>(file);
            case "csv":
                return new CSVWalReader<>(file);
            default:
                throw new RuntimeException("Fail to create wal reader. Unknown passed codec " + codec + " of wal file " + file.name());
        }
    }

    public static <T extends DataModel> WalWriter<T> getWriter(WalFile file) throws IOException {
        String codec = file.codec();
        switch(codec){
            case "object":
                return new ObjectWriter<>(file);
            case "json":
                return new JSONWriter<>(file);
            case "csv":
                return new CSVWalWriter<>(file);
            default:
                throw new RuntimeException("Fail to create wal writer. Unknown passed codec " + codec + " of wal file " + file.name());
        }
    }

    public static synchronized WalFile getShortestWalFile(String walFolder){
        String codec = Config.getProperty(Const.WAL_WRITER_CODEC);
        Long maxSize = Config.getLongProperty(Const.WAL_MAX_SIZE, 1048576);

        File dir = new File(walFolder);
        if(!dir.isDirectory() || !dir.exists())
            throw new RuntimeException(walFolder + " is not a real directory or is not existing");

        File[] files = dir.listFiles();
        String fileName = null;
        Long size = Long.MAX_VALUE;
        for(File file : Objects.requireNonNull(files)){
            Long length = file.length();
            if(length < maxSize && length < size){
                fileName = file.getAbsolutePath();
                size = length;
            }
        }

        String walType = Config.getProperty(Const.WAL_FILE_TYPE);
        if (fileName == null){
            logger.info("No unfinished wal found. Tend to create new wal file");
            fileName = walFolder + WalFile.newFileName();
        }

        switch(walType){
            case "default":
                return new LocalWalFile(fileName, codec, maxSize);
            default:
                throw new RuntimeException("Unsupported wal type: " + walType);
        }
    }
}
